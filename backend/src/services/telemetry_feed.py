"""Transactional telemetry change feed and resumable subscriptions.

Ordering contract
-----------------
A telemetry write publishes its event in the *same database transaction* as the
data mutation, and allocates its feed sequence under an exclusive lock on the
single ``telemetry_seq`` row. The lock is released only when that transaction
ends, so commit order and ``seq`` order are identical: a change whose sequence
is lower can never commit after one whose sequence is higher. A consumer that
walks the feed by ``seq`` therefore cannot skip an event that commits late,
which is exactly the lost-event failure a naive auto-increment cursor suffers
when two reports commit out of order.

Because the sequence bump shares the transaction, a rolled-back write publishes
nothing and its sequence value is rolled back with it, so the feed has neither
phantom events nor gaps. The lock is held for the writer's own short transaction
only; reads (history replay and polling) never take it, so consumers do not
block writers, and concurrent reports still commit normally.

Subscription resume contract
----------------------------
At creation a subscription records a cut-over point ``seq_start`` and the
highest committed telemetry id ``backfill_high``, captured atomically under the
same sequence lock. Its first phase replays existing telemetry in ``id`` order
up to ``backfill_high`` in slices of ``batch_size``; every change with
``seq > seq_start`` is kept in the shared append-only feed and delivered only
after the history read finishes. No per-subscription copy of the table or feed
is ever made.

Each produced batch is persisted *before* it is handed to the downstream, and
the subscription cursor advances only when that batch is explicitly
acknowledged. A batch delivered but not acknowledged survives a restart and is
replayed with the same batch id and exactly the same events; acknowledging
anything other than the pending head is rejected, so progress can never skip
data. Delivery is at-least-once: a correction that lands while the history read
has not yet reached that row is observed both in the history slice and as an
``update`` event.
"""
from fastapi import HTTPException
from sqlalchemy import func,text
from src.config.database import Session
from src.models.feed import Subscription,SubscriptionBatch,TelemetryChange
from src.models.telemetry import Telemetry
from src.utils.serialization import record
def _ensure_seq(s):
 """Lock the single sequence row until commit and return its current value.

 The ``FOR UPDATE`` on the next line is the whole serialisation mechanism:
 writers queue here for one short row lock and readers never take it.
 """
 row=s.execute(text('SELECT value FROM telemetry_seq WHERE id=1 FOR UPDATE')).first()
 if row is not None:return int(row[0])
 # Only reachable when the database was not seeded (SEED_DEMO=false) or
 # predates the feed. Bootstrap once, then re-read under the same row lock so
 # the value seen is the latest committed one even if another writer won the race.
 current=int(s.execute(text('SELECT COALESCE(MAX(seq),0) FROM telemetry_changes')).scalar() or 0)
 s.execute(text('INSERT IGNORE INTO telemetry_seq(id,value) VALUES(1,:v)'),{'v':current})
 return int(s.execute(text('SELECT value FROM telemetry_seq WHERE id=1 FOR UPDATE')).scalar())
def _publish(s,op,telemetry_id,payload):
 """Bump the feed sequence inside the caller's transaction. Caller commits."""
 seq=_ensure_seq(s)+1
 s.execute(text('UPDATE telemetry_seq SET value=:v WHERE id=1'),{'v':seq})
 s.add(TelemetryChange(seq=seq,telemetry_id=telemetry_id,op=op,payload=payload))
 return seq
def _snapshot(row):
 return {'id':row.id,'sensor_id':row.sensor_id,'recorded_at':row.recorded_at,'device_type':row.device_type,'value':row.value}
def backfill(data):
 """补录：insert a late telemetry record and publish it atomically."""
 with Session() as s:
  row=Telemetry(sensor_id=data.sensor_id,recorded_at=data.recorded_at,device_type=data.device_type,value=data.value)
  s.add(row);s.flush()
  _publish(s,'insert',row.id,_snapshot(row))
  s.commit();s.refresh(row);return record(row)
def correct(telemetry_id,data):
 """更正：append an update event carrying the full post-change row."""
 changes={k:v for k,v in data.model_dump().items() if v is not None}
 with Session() as s:
  row=s.query(Telemetry).filter(Telemetry.id==telemetry_id).with_for_update().first()
  if not row:raise HTTPException(404,'未找到遥测记录')
  for k,v in changes.items():setattr(row,k,v)
  s.flush()
  _publish(s,'update',row.id,_snapshot(row))
  s.commit();s.refresh(row);return record(row)
def create_subscription(data):
 with Session() as s:
  seq_start=_ensure_seq(s)  # exclusive lock first: no write can commit mid-cutover
  if s.query(Subscription).filter_by(name=data.name).first():raise HTTPException(409,'订阅名称已存在')
  highest=s.execute(text('SELECT COALESCE(MAX(id),0) FROM telemetry')).scalar() or 0
  row=Subscription(name=data.name,from_id=data.from_id,batch_size=data.batch_size,phase='history',backfill_high=int(highest),seq_start=seq_start,backfill_cursor=data.from_id-1,seq_cursor=seq_start)
  s.add(row);s.commit();s.refresh(row);return record(row)
def list_subscriptions():
 with Session() as s:return [record(r) for r in s.query(Subscription).order_by(Subscription.id).all()]
def detail(subscription_id):
 with Session() as s:
  row=s.get(Subscription,subscription_id)
  if not row:raise HTTPException(404,'未找到订阅')
  return record(row)
def _pending(s,subscription_id):
 return s.query(SubscriptionBatch).filter(SubscriptionBatch.subscription_id==subscription_id,SubscriptionBatch.acked_at.is_(None)).order_by(SubscriptionBatch.id).first()
def _history_slice(s,sub):
 rows=s.query(Telemetry).filter(Telemetry.id>sub.backfill_cursor,Telemetry.id<=sub.backfill_high).order_by(Telemetry.id).limit(sub.batch_size).all()
 return [{'seq':None,'op':'snapshot','telemetry_id':r.id,'data':record(r)} for r in rows],(rows[-1].id if rows else sub.backfill_cursor)
def _changes_slice(s,sub):
 rows=s.query(TelemetryChange).filter(TelemetryChange.seq>sub.seq_cursor,TelemetryChange.seq>sub.seq_start).order_by(TelemetryChange.seq).limit(sub.batch_size).all()
 return [{'seq':r.seq,'op':r.op,'telemetry_id':r.telemetry_id,'data':r.payload} for r in rows],(rows[-1].seq if rows else sub.seq_cursor)
def next_batch(subscription_id):
 """Return the next unacknowledged batch, or the pending one unchanged."""
 with Session() as s:
  sub=s.query(Subscription).filter(Subscription.id==subscription_id).with_for_update().first()
  if not sub:raise HTTPException(404,'未找到订阅')
  pending=_pending(s,subscription_id)
  if pending:
   pending.attempts+=1;s.commit();s.refresh(pending);s.refresh(sub)
   return {'batch':{**record(pending),'replayed':True},'subscription':record(sub)}
  while sub.phase=='history' and sub.backfill_cursor>=sub.backfill_high:
   sub.phase='changes'
  if sub.phase=='history':
   events,to_cursor=_history_slice(s,sub)
   last=to_cursor>=sub.backfill_high
   if not events:
    # Every remaining id was deleted: skip the gap without emitting a batch.
    sub.backfill_cursor=sub.backfill_high;sub.phase='changes'
    events,to_cursor=_changes_slice(s,sub)
    if not events:
     s.commit()
     return {'batch':None,'subscription':record(sub),'caught_up':True}
    batch=SubscriptionBatch(subscription_id=sub.id,kind='changes',from_cursor=sub.seq_cursor,to_cursor=to_cursor,last_backfill=False,events=events,event_count=len(events))
   else:
    batch=SubscriptionBatch(subscription_id=sub.id,kind='history',from_cursor=sub.backfill_cursor,to_cursor=to_cursor,last_backfill=last,events=events,event_count=len(events))
  else:
   events,to_cursor=_changes_slice(s,sub)
   if not events:
    s.commit()
    return {'batch':None,'subscription':record(sub),'caught_up':True}
   batch=SubscriptionBatch(subscription_id=sub.id,kind='changes',from_cursor=sub.seq_cursor,to_cursor=to_cursor,last_backfill=False,events=events,event_count=len(events))
  s.add(batch);s.commit();s.refresh(batch);s.refresh(sub)
  return {'batch':{**record(batch),'replayed':False},'subscription':record(sub)}
def _advance(sub,batch):
 if batch.kind=='history':
  sub.backfill_cursor=batch.to_cursor
  if batch.last_backfill:sub.phase='changes'
 else:
  sub.seq_cursor=batch.to_cursor
def ack(subscription_id,batch_id):
 with Session() as s:
  sub=s.query(Subscription).filter(Subscription.id==subscription_id).with_for_update().first()
  if not sub:raise HTTPException(404,'未找到订阅')
  batch=s.get(SubscriptionBatch,batch_id)
  if not batch or batch.subscription_id!=subscription_id:raise HTTPException(404,'未找到待确认批次')
  if batch.acked_at:return {'batch_id':batch_id,'duplicate':True,'subscription':record(sub)}
  head=_pending(s,subscription_id)
  if not head or head.id!=batch.id:raise HTTPException(409,'必须按顺序确认批次，当前待确认批次不匹配')
  batch.acked_at=func.now()
  _advance(sub,batch)
  # Keep only the newest acknowledged batch for duplicate-ack tolerance; the
  # delivered payload itself is not retained long-term, so a subscription does
  # not accumulate a private copy of the telemetry table.
  s.query(SubscriptionBatch).filter(SubscriptionBatch.subscription_id==subscription_id,SubscriptionBatch.acked_at.isnot(None),SubscriptionBatch.id<batch.id).delete(synchronize_session=False)
  s.commit();s.refresh(sub)
  return {'batch_id':batch_id,'duplicate':False,'subscription':record(sub)}
