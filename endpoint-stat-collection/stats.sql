-- Copyright (c) Aaron Gallagher <_@habnab.it>
-- See COPYING for details.

CREATE TABLE endpoint_stats (
  recorded_at REAL NOT NULL,
  endpoint TEXT NOT NULL,
  length REAL NOT NULL
);
