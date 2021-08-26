#!/bin/sh
PGPASSWORD='root123' psql -h /var/run/postgresql -U root < /room.sql
