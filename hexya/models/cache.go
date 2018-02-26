// Copyright 2016 NDP Systèmes. All Rights Reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package models

import (
	"errors"
	"strings"

	"github.com/hexya-erp/hexya/hexya/models/fieldtype"
)

// A cacheRef is a key to find a record in a cache
type cacheRef struct {
	model *Model
	id    int64
}

// A cache holds records field values for caching the database to
// improve performance. cache is not safe for concurrent access.
type cache struct {
	counterID       int64
	data            map[cacheRef]*FieldMap
	m2mLinks        map[*Model]map[[2]int64]bool
	scheduledInsert map[cacheRef]cacheRef
	scheduledUpdate map[cacheRef]map[string]bool
}

func (c *cache) isInDb(ref cacheRef) bool {
	return !c.isNotInDb(ref)
}

func (c *cache) isNotInDb(ref cacheRef) bool {
	insertedRef := c.scheduledInsert[ref]
	return ref.id <= 0 && insertedRef.id <= 0
}

// updateEntry creates or updates an entry in the cache defined by its model, id and fieldName.
// fieldName can be a path
func (c *cache) updateEntry(mi *Model, id int64, fieldName string, value interface{}) error {
	ref, fName, err := c.getRelatedRef(mi, id, fieldName)
	if err != nil {
		return err
	}
	c.updateEntryByRef(ref, fName, value)
	return nil
}

func (c *cache) filterIdInCache(rc *RecordCollection) (*RecordCollection, *RecordCollection) {
	var idsInCache, idsNotInCache []int64
	for _, id := range rc.ids {
		if _, found := c.data[c.getCacheRef(rc.model, id)]; found {
			idsInCache = append(idsInCache, c.getCacheRef(rc.model, id).id)
		} else {
			idsNotInCache = append(idsNotInCache, c.getCacheRef(rc.model, id).id)
		}
	}
	return rc.env.Pool(rc.ModelName()).withIds(idsInCache), rc.env.Pool(rc.ModelName()).withIds(idsNotInCache)
}

//Get the data by the ref and init it if not exist
func (c *cache) getData(ref cacheRef) FieldMap {
	if _, ok := c.data[ref]; !ok {
		v := make(FieldMap)
		c.data[ref] = &v
		(*c.data[ref])["id"] = ref.id
	}
	return *c.data[ref]
}

func (c *cache) initWithData(ref cacheRef, data FieldMap) FieldMap {
	c.data[ref] = &data
	(*c.data[ref])["id"] = ref.id
	return *c.data[ref]
}

// updateEntryByRef creates or updates an entry to the cache from a cacheRef
// and a field json name (no path).
func (c *cache) updateEntryByRef(ref cacheRef, jsonName string, value interface{}) {
	c.getData(ref)
	if ref.id > 0 {
		if _, ok := c.scheduledUpdate[ref]; !ok {
			c.scheduledUpdate[ref] = make(map[string]bool)
		}
		c.scheduledUpdate[ref][jsonName] = true
	}
	fi := ref.model.fields.MustGet(jsonName)
	switch fi.fieldType {
	case fieldtype.One2Many:
		ids := value.([]int64)
		for _, id := range ids {
			c.updateEntry(fi.relatedModel, id, fi.jsonReverseFK, ref.id)
		}
		c.getData(ref)[jsonName] = true
	case fieldtype.Rev2One:
		id := value.(int64)
		c.updateEntry(fi.relatedModel, id, fi.jsonReverseFK, ref.id)
		c.getData(ref)[jsonName] = true
	case fieldtype.Many2Many:
		ids := value.([]int64)
		c.removeM2MLinks(fi, ref.id)
		c.addM2MLink(fi, ref.id, ids)
		c.getData(ref)[jsonName] = true
	default:
		c.getData(ref)[jsonName] = value
	}
}

// removeM2MLinks removes all M2M links associated with the record with
// the given id on the given field
func (c *cache) removeM2MLinks(fi *Field, id int64) {
	if _, exists := c.m2mLinks[fi.m2mRelModel]; !exists {
		return
	}
	index := (strings.Compare(fi.m2mOurField.name, fi.m2mTheirField.name) + 1) / 2
	for link := range c.m2mLinks[fi.m2mRelModel] {
		if link[index] == id {
			delete(c.m2mLinks[fi.m2mRelModel], link)
		}
	}
}

// addM2MLink adds an M2M link between this record with its given ID
// and the records given by values on the given field.
func (c *cache) addM2MLink(fi *Field, id int64, values []int64) {
	if _, exists := c.m2mLinks[fi.m2mRelModel]; !exists {
		c.m2mLinks[fi.m2mRelModel] = make(map[[2]int64]bool)
	}
	ourIndex := (strings.Compare(fi.m2mOurField.name, fi.m2mTheirField.name) + 1) / 2
	theirIndex := (ourIndex + 1) % 2
	for _, val := range values {
		var newLink [2]int64
		newLink[ourIndex] = id
		newLink[theirIndex] = val
		c.m2mLinks[fi.m2mRelModel][newLink] = true
	}
}

// getM2MLinks returns the linked ids to this id through the given field.
func (c *cache) getM2MLinks(fi *Field, id int64) []int64 {
	if _, exists := c.m2mLinks[fi.m2mRelModel]; !exists {
		return []int64{}
	}
	var res []int64
	ourIndex := (strings.Compare(fi.m2mOurField.name, fi.m2mTheirField.name) + 1) / 2
	theirIndex := (ourIndex + 1) % 2
	for link := range c.m2mLinks[fi.m2mRelModel] {
		if link[ourIndex] == id {
			res = append(res, link[theirIndex])
		}
	}
	return res
}

// addRecord successively adds each entry of the given FieldMap to the cache.
// fMap keys may be a paths relative to this Model (e.g. "User.Profile.Age").
func (c *cache) addRecord(mi *Model, id int64, fMap FieldMap) {
	paths := make(map[int][]string)
	var maxLen int
	// We create our exprsMap with the length of the path as key
	for _, path := range fMap.Keys() {
		exprs := strings.Split(path, ExprSep)
		paths[len(exprs)] = append(paths[len(exprs)], path)
		if len(exprs) > maxLen {
			maxLen = len(exprs)
		}
	}
	// We add entries into the cache, starting from the smallest paths
	for i := 0; i <= maxLen; i++ {
		for _, path := range paths[i] {
			c.updateEntry(mi, id, path, fMap[path])
		}
	}
}

// invalidateRecord removes an entire record from the cache
//
// WARNING: Reload the record as soon as possible after calling
// this method, since this will bring discrepancies in the other
// records references (One2Many and Many2Many fields).
func (c *cache) invalidateRecord(mi *Model, id int64) {
	delete(c.data, c.getCacheRef(mi, id))
	for _, fi := range mi.fields.registryByJSON {
		if fi.fieldType == fieldtype.Many2Many {
			c.removeM2MLinks(fi, id)
		}
	}
}

// removeEntry removes the given entry from cache
func (c *cache) removeEntry(mi *Model, id int64, fieldName string) {
	if !c.checkIfInCache(mi, []int64{id}, []string{fieldName}) {
		return
	}
	delete(c.getData(c.getCacheRef(mi, id)), fieldName)
	fi := mi.fields.MustGet(fieldName)
	if fi.fieldType == fieldtype.Many2Many {
		c.removeM2MLinks(fi, id)
	}
}

// get returns the cache value of the given fieldName
// for the given modelName and id. fieldName may be a path
// relative to this Model (e.g. "User.Profile.Age").
//
// If the requested value cannot be found, get returns nil
func (c *cache) get(mi *Model, id int64, fieldName string) interface{} {
	ref, fName, err := c.getRelatedRef(mi, id, fieldName)
	if err != nil {
		return nil
	}
	fi := ref.model.fields.MustGet(fName)
	switch fi.fieldType {
	case fieldtype.One2Many:
		var relIds []int64
		for cRef, cVal := range c.data {
			if cRef.model != fi.relatedModel {
				continue
			}
			if (*cVal)[fi.jsonReverseFK] != ref.id {
				continue
			}
			relIds = append(relIds, cRef.id)
		}
		return relIds
	case fieldtype.Rev2One:
		for cRef, cVal := range c.data {
			if cRef.model != fi.relatedModel {
				continue
			}
			if (*cVal)[fi.jsonReverseFK] != ref.id {
				continue
			}
			return cRef.id
		}
		return nil
	case fieldtype.Many2Many:
		return c.getM2MLinks(fi, ref.id)
	default:
		return c.getData(ref)[fName]
	}
}

// getRecord returns the whole record specified by modelName and id
// as it is currently in cache.
func (c *cache) getRecord(model *Model, id int64) FieldMap {
	res := make(FieldMap)
	ref := model.toRef(id)
	for _, fName := range c.getData(ref).Keys() {
		res[fName] = c.get(model, id, fName)
	}
	return res
}

// checkIfInCache returns true if all fields given by fieldNames are available
// in cache for all the records with the given ids in the given model.
func (c *cache) checkIfInCache(mi *Model, ids []int64, fieldNames []string) bool {
	for _, id := range ids {
		if id < 0 {
			continue
		}
		for _, fName := range fieldNames {
			ref, path, err := c.getRelatedRef(mi, id, fName)
			if err != nil {
				return false
			}
			if _, ok := c.getData(ref)[path]; !ok {
				return false
			}
		}
	}
	return true
}

func (c *cache) copyPointer(from cacheRef, to cacheRef) {
	c.data[to] = c.data[from]
}

// getRelatedRef returns the cacheRef and field name of the field that is
// defined by path when walking from the given model with the given ID.
func (c *cache) getRelatedRef(mi *Model, id int64, path string) (cacheRef, string, error) {
	exprs := jsonizeExpr(mi, strings.Split(path, ExprSep))
	if len(exprs) > 1 {
		relMI := mi.getRelatedModelInfo(exprs[0])
		fkID, ok := c.get(mi, id, exprs[0]).(int64)
		if !ok {
			return cacheRef{}, "", errors.New("requested value not in cache")
		}
		return c.getRelatedRef(relMI, fkID, strings.Join(exprs[1:], ExprSep))
	}
	return mi.toRef(id), exprs[0], nil
}

func (c *cache) getCacheRef(mi *Model, id int64) cacheRef {
	return cacheRef{model: mi, id: id}
}

// newCache creates a pointer to a new cache instance.
func newCache() *cache {
	res := cache{
		data:            make(map[cacheRef]*FieldMap),
		m2mLinks:        make(map[*Model]map[[2]int64]bool),
		scheduledInsert: make(map[cacheRef]cacheRef),
		scheduledUpdate: make(map[cacheRef]map[string]bool),
	}
	return &res
}
