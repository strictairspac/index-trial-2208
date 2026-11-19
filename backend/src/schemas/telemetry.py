from datetime import datetime
from pydantic import BaseModel,Field,model_validator
from src.constants.enums import DEVICE_TYPES
def _timestamp(value):
 try:return datetime.fromisoformat(value).isoformat(timespec='seconds')
 except ValueError:raise ValueError('时间格式无效，请使用 ISO-8601')
class BackfillInput(BaseModel):
 sensor_id:int=Field(ge=1,le=1000)
 recorded_at:str
 device_type:str
 value:float
 @model_validator(mode='after')
 def valid(self):
  if self.device_type not in DEVICE_TYPES:raise ValueError('设备类型无效')
  self.recorded_at=_timestamp(self.recorded_at)
  return self
class CorrectionInput(BaseModel):
 sensor_id:int|None=Field(default=None,ge=1,le=1000)
 recorded_at:str|None=None
 device_type:str|None=None
 value:float|None=None
 @model_validator(mode='after')
 def valid(self):
  if all(x is None for x in (self.sensor_id,self.recorded_at,self.device_type,self.value)):raise ValueError('至少提供一个更正字段')
  if self.recorded_at is not None:self.recorded_at=_timestamp(self.recorded_at)
  if self.device_type is not None and self.device_type not in DEVICE_TYPES:raise ValueError('设备类型无效')
  return self
class SubscriptionInput(BaseModel):
 name:str=Field(min_length=2,max_length=120)
 from_id:int=Field(default=1,ge=1)
 batch_size:int=Field(default=200,ge=1,le=1000)
