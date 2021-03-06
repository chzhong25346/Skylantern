# -*- coding: utf-8 -*-

from .db.db import Db as db


class Index(db.Model):
    __tablename__ = 'index'
    symbol = db.Column(db.String(10), unique=True, nullable=False, primary_key=True)
    company = db.Column(db.String(100),nullable=False)
    quote = db.relationship('Quote', backref='quote', lazy=True)
    report = db.relationship('Report', backref='report', lazy=True)


class Quote(db.Model):
    __tablename__ = 'quote'
    id = db.Column(db.String(40), unique=True, nullable=False, primary_key=True)
    symbol = db.Column(db.String(10), db.ForeignKey("index.symbol"), nullable=False)
    date = db.Column(db.DateTime, nullable=False)
    open = db.Column(db.Float, nullable=True)
    high = db.Column(db.Float, nullable=True)
    low = db.Column(db.Float, nullable=True)
    close = db.Column(db.Float, nullable=True)
    volume = db.Column(db.BIGINT, nullable=True)


class Report(db.Model):
    __tablename__ = 'report'
    id = db.Column(db.String(40), unique=True, nullable=False, primary_key=True)
    date = db.Column(db.DateTime, nullable=False)
    symbol = db.Column(db.String(10), db.ForeignKey("index.symbol"), nullable=False)
    yr_high = db.Column(db.Boolean, nullable=True)
    yr_low = db.Column(db.Boolean, nullable=True)
    downtrend = db.Column(db.Boolean, nullable=True)
    uptrend = db.Column(db.Boolean, nullable=True)
    high_volume = db.Column(db.Boolean, nullable=True)
    rsi = db.Column(db.String(4), nullable=True)
    macd = db.Column(db.String(4), nullable=True)
    bolling = db.Column(db.String(10), nullable=True)
    # adx = db.Column(db.String(4), nullable=True)


class Holding(db.Model):
    __tablename__ = 'holding'
    symbol = db.Column(db.String(10), db.ForeignKey("index.symbol"), primary_key=True, nullable=False)
    avg_cost = db.Column(db.Float, nullable=True)
    book_value = db.Column(db.Float, nullable=True)
    change_dollar = db.Column(db.Float, nullable=True)
    change_percent = db.Column(db.Float, nullable=True)
    mkt_price = db.Column(db.Float, nullable=True)
    mkt_value = db.Column(db.Float, nullable=True)
    quantity = db.Column(db.BIGINT, nullable=True)
    note = db.Column(db.String(20), nullable=True)


class Transaction(db.Model):
    __tablename__ = 'transaction'
    id = db.Column(db.String(40), unique=True, nullable=False, primary_key=True)
    symbol = db.Column(db.String(10), db.ForeignKey("index.symbol"), nullable=False)
    date = db.Column(db.DateTime, nullable=False)
    price = db.Column(db.Float, nullable=True)
    quantity = db.Column(db.BIGINT, nullable=True)
    settlement = db.Column(db.Float, nullable=True)
    type = db.Column(db.String(6), nullable=False)


# EIA Models
class Eia_price(db.Model):
    __tablename__ = 'eia_price'
    id = db.Column(db.String(40), unique=True, nullable=False, primary_key=True)
    sid = db.Column(db.String(50), nullable=False)
    date = db.Column(db.DateTime, nullable=False)
    value = db.Column(db.Float, nullable=True)


# EIA Models
class Eia_storage(db.Model):
    __tablename__ = 'eia_storage'
    id = db.Column(db.String(40), unique=True, nullable=False, primary_key=True)
    sid = db.Column(db.String(50), nullable=False)
    date = db.Column(db.DateTime, nullable=False)
    value = db.Column(db.BIGINT, nullable=True)
