from fastapi import APIRouter,Depends
from src.middlewares.auth import current,writer
from src.schemas.telemetry import BackfillInput,CorrectionInput,SubscriptionInput
from src.services import telemetry_feed
from src.services.audit import log
router=APIRouter()
@router.post('/telemetry')
def backfill(data:BackfillInput,u=Depends(writer)):
 row=telemetry_feed.backfill(data);log(u.email,'telemetry.backfill',str(row['id']));return row
@router.patch('/telemetry/{item_id}')
def correct(item_id:int,data:CorrectionInput,u=Depends(writer)):
 row=telemetry_feed.correct(item_id,data);log(u.email,'telemetry.correct',str(row['id']));return row
@router.post('/subscriptions')
def create(data:SubscriptionInput,u=Depends(writer)):
 row=telemetry_feed.create_subscription(data);log(u.email,'subscription.create',row['name']);return row
@router.get('/subscriptions')
def listing(u=Depends(current)):return telemetry_feed.list_subscriptions()
@router.get('/subscriptions/{subscription_id}')
def detail(subscription_id:int,u=Depends(current)):return telemetry_feed.detail(subscription_id)
@router.post('/subscriptions/{subscription_id}/next')
def next_batch(subscription_id:int,u=Depends(writer)):return telemetry_feed.next_batch(subscription_id)
@router.post('/subscriptions/{subscription_id}/batches/{batch_id}/ack')
def ack(subscription_id:int,batch_id:int,u=Depends(writer)):return telemetry_feed.ack(subscription_id,batch_id)
