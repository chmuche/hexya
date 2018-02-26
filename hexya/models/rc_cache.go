package models

//createInCache init a new record with the data in cahce
//return a negative int64
func (rc *RecordCollection) createInCache(data FieldMapper) int64 {
	rc.env.cache.counterID--
	id := rc.getCacheRef(rc.env.cache.counterID)
	rc.env.cache.initWithData(id, data.FieldMap())
	rc.env.cache.scheduledInsert[id] = cacheRef{}
	return id.id
}

func (rc *RecordCollection) getCacheRef(id int64) cacheRef {
	return cacheRef{model: rc.model, id: id}
}

func (rc *RecordCollection) getFirstCacheRef() cacheRef {
	return cacheRef{model: rc.model, id: rc.ids[0]}
}

//func (rc *RecordCollection) getAllCacheRef() []cacheRef {
//	refs := [len(rc.ids)]cacheRef
//	return cacheRef{model: rc.model, id: rc.ids[0]}
//}

func (m *Model) toRef(id int64) cacheRef {
	return cacheRef{model: m, id: id}
}
