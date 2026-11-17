from hypothesis import given,strategies as st
from datapipe.transforms.deduplication import deduplicate
from datapipe.transforms.filtering import filter_records
from datapipe.transforms.mapping import map_record
from datapipe.transforms.aggregation import aggregate_daily
def test_mapping():assert map_record({"a":1,"b":2},{"a":"x"})=={"x":1,"b":2}
def test_filtering():assert len(filter_records([{"x":1},{"x":2}],"x","gt",1))==1
def test_dedup_first_seen():assert deduplicate([{"id":1,"v":"first"},{"id":1,"v":"last"}],"id")==[{"id":1,"v":"first"}]
def test_aggregation():assert aggregate_daily([{"date":"2026-01-01","x":2},{"date":"2026-01-01","x":3}],"date",{"x":"sum"})==[{"date":"2026-01-01","x_sum":5.0}]
@given(st.lists(st.integers(),max_size=100))
def test_dedupe_idempotent(values):
 rows=[{"id":v} for v in values];once=deduplicate(rows,"id");assert deduplicate(once,"id")==once;assert len(once)<=len(rows)
@given(st.lists(st.integers(),max_size=100))
def test_filter_never_increases(values):assert len(filter_records([{"x":v} for v in values],"x","gt",0))<=len(values)
