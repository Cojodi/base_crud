# -*- coding: utf-8 -*-
from typing import Optional

from pydantic import validate_model
from sqlalchemy.orm import selectinload
from sqlmodel import SQLModel, select


def _update(self, obj: Optional[SQLModel], **kwargs):
    if obj is not None:
        for k, v in obj.dict().items():
            setattr(self, k, v)

    for k, v in kwargs.items():
        setattr(self, k, v)

    *_, validation_error = validate_model(self.__class__, self.__dict__)
    if validation_error:
        raise validation_error

    return self


setattr(SQLModel, "update", _update)


class Base:
    def __init__(self, db, Table):
        self.db = db
        self.Table = Table

    def build_q(self, kwargs, order_by):
        q = select(self.Table)
        for attr_name, attr_value in kwargs.items():
            # special key for relationship loading
            if attr_name.startswith("with_"):
                q = q.options(
                    selectinload(getattr(self.Table, attr_name.replace("with_", "")))
                )
            else:
                q = q.where(getattr(self.Table, attr_name) == attr_value)

        for order_by_ in order_by:
            q = q.order_by(order_by_)

        return q


class BaseCRUD(Base):
    def __init__(self, db, Table):
        super().__init__(db, Table)

    def create(self, *, orm: Optional[SQLModel] = None, **kwargs):
        if orm is not None:
            kwargs.update(orm.dict())

        o = self.Table(**kwargs)

        self.db.add(o)

        return o

    def get_else_create(self, *, orm: Optional[SQLModel] = None, **kwargs):
        raise NotImplementedError()

    # o is an obj type self.Table
    def update(self, o, *, orm: Optional[SQLModel] = None, **kwargs):
        return o.update(orm, **kwargs)

    def first(self, *, order_by=[], **kwargs):
        q = self.build_q(kwargs, order_by)
        return self.db.exec(q).first()

    def one(self, *, order_by=[], **kwargs):
        q = self.build_q(kwargs, order_by)
        return self.db.exec(q).one()

    def all(self, *, order_by=[], **kwargs):
        q = self.build_q(kwargs, order_by)
        return self.db.exec(q).all()


class AsyncBaseCRUD(BaseCRUD):
    def __init__(self, db, Table):
        super().__init__(db, Table)

    async def first(self, *, order_by=[], **kwargs):
        q = self.build_q(kwargs, order_by)
        return (await self.db.exec(q)).first()

    async def one(self, *, order_by=[], **kwargs):
        q = self.build_q(kwargs, order_by)
        return (await self.db.exec(q)).one()

    async def all(self, *, order_by=[], **kwargs):
        q = self.build_q(kwargs, order_by)
        return (await self.db.exec(q)).all()

    async def get_else_create(self, *, orm: SQLModel = None, **kwargs):
        raise NotImplementedError()
