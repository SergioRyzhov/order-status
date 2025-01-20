from fastapi import APIRouter, HTTPException, Depends
from sqlalchemy.orm import Session
from pydantic import BaseModel
from app.kafka_producer import send_to_kafka
from app.db import get_db, save_order
from app.models.order import Order

router = APIRouter()

class OrderBase(BaseModel):
    order_id: int
    item_name: str
    quantity: int

    class Config:
        from_attributes=True

@router.post('/orders')
async def create_order(order: OrderBase, db: Session = Depends(get_db)):
    try:
        saved_order = save_order(db, order.dict())
        send_to_kafka('topic_orders', order.dict())
        return {'status': 'success', 'order_id': saved_order.id}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f'Failed to process order: {str(e)}')

@router.get('/orders')
async def get_all_orders(db: Session = Depends(get_db)):
    try:
        orders = db.query(Order).all()
        return {'orders': [OrderBase.from_orm(order) for order in orders]}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f'Failed to fetch orders: {str(e)}')

@router.get('/')
async def health_check():
    return {'status': 'ok'}
