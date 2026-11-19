from sqlalchemy import BigInteger,Boolean,Column,DateTime,ForeignKey,Integer,JSON,String,text
from src.config.database import Base
class TelemetrySeq(Base):
 __tablename__='telemetry_seq'
 id=Column(Integer,primary_key=True)
 value=Column(BigInteger,nullable=False,default=0)
class TelemetryChange(Base):
 __tablename__='telemetry_changes'
 seq=Column(BigInteger,primary_key=True,autoincrement=False)
 telemetry_id=Column(Integer,nullable=False,index=True)
 op=Column(String(16),nullable=False)
 payload=Column(JSON,nullable=False)
 changed_at=Column(DateTime(6),nullable=False,server_default=text('CURRENT_TIMESTAMP(6)'))
class Subscription(Base):
 __tablename__='telemetry_subscriptions'
 id=Column(Integer,primary_key=True)
 name=Column(String(120),unique=True,nullable=False)
 from_id=Column(Integer,nullable=False,default=1)
 batch_size=Column(Integer,nullable=False,default=200)
 phase=Column(String(16),nullable=False,default='history')
 backfill_high=Column(Integer,nullable=False)
 seq_start=Column(BigInteger,nullable=False)
 backfill_cursor=Column(Integer,nullable=False)
 seq_cursor=Column(BigInteger,nullable=False)
 created_at=Column(DateTime(6),nullable=False,server_default=text('CURRENT_TIMESTAMP(6)'))
class SubscriptionBatch(Base):
 __tablename__='telemetry_subscription_batches'
 id=Column(Integer,primary_key=True)
 subscription_id=Column(Integer,ForeignKey('telemetry_subscriptions.id'),nullable=False,index=True)
 kind=Column(String(16),nullable=False)
 from_cursor=Column(BigInteger,nullable=False)
 to_cursor=Column(BigInteger,nullable=False)
 last_backfill=Column(Boolean,nullable=False,default=False)
 events=Column(JSON,nullable=False)
 event_count=Column(Integer,nullable=False)
 attempts=Column(Integer,nullable=False,default=0)
 created_at=Column(DateTime(6),nullable=False,server_default=text('CURRENT_TIMESTAMP(6)'))
 acked_at=Column(DateTime(6),nullable=True)
