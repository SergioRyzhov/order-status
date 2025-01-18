from sqlalchemy.orm import Session
from app.models.order import SessionLocal, Order

def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()

def save_order(db: Session, order_data: dict):
    order = Order(
        order_id=order_data["order_id"],
        item_name=order_data["item_name"],
        quantity=order_data["quantity"],
    )
    db.add(order)
    db.commit()
    db.refresh(order)
    return order
