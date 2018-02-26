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
	"github.com/hexya-erp/hexya/hexya/models/types"
	"github.com/hexya-erp/hexya/hexya/tools/logging"
)

// DBSerializationMaxRetries defines the number of time a
// transaction that failed due to serialization error should
// be retried.
const DBSerializationMaxRetries uint8 = 5

// An Environment stores various contextual data used by the models:
// - the database cursor (current open transaction),
// - the current user ID (for access rights checking)
// - the current context (for storing arbitrary metadata).
// The Environment also stores caches.
type Environment struct {
	cr        *Cursor
	uid       int64
	context   *types.Context
	cache     *cache
	callStack []*methodLayer
	super     *methodLayer
	retries   uint8
}

// Cr returns a pointer to the Cursor of the Environment
func (env Environment) Cr() *Cursor {
	return env.cr
}

// Uid returns the user id of the Environment
func (env Environment) Uid() int64 {
	return env.uid
}

// Context returns the Context of the Environment
func (env Environment) Context() *types.Context {
	return env.context
}

// Flush returns a pointer to the Cursor of the Environment
func (env Environment) Flush() {
	env.flush()
}

func (env Environment) flush() {
	for e := range env.cache.scheduledInsert {
		env.insertData(e)
	}
	for ref, fields := range env.cache.scheduledUpdate {
		rc := env.Pool(ref.model.name).withIds([]int64{ref.id})
		fMap := make(FieldMap)
		for fieldName := range fields {
			fMap[fieldName] = env.cache.getData(ref)[fieldName]
		}
		sql, args := rc.query.updateQuery(fMap)
		res := rc.env.cr.Execute(sql, args...)
		if num, _ := res.RowsAffected(); num == 0 {
			log.Panic("Trying to update an empty RecordSet", "model", rc.ModelName(), "values", fMap)
		}
	}
}

func (env Environment) insertData(ref cacheRef) {
	if env.cache.isInDb(ref) {
		return
	}
	//force the external id ?
	rc := env.Pool(ref.model.name).withIds([]int64{ref.id})
	for field, value := range env.cache.getData(ref) {
		fi := rc.query.recordSet.model.fields.MustGet(field)
		if fi.fieldType.IsFKRelationType() && value != nil {
			fkRef := fi.relatedModel.toRef(value.(int64))
			if env.cache.isNotInDb(fkRef) {
				env.insertData(fkRef)
			}
			env.cache.updateEntryByRef(ref, field, env.cache.scheduledInsert[fkRef].id)
		}
	}
	var createdId int64
	sql, args := rc.query.insertQuery(env.cache.getData(ref))
	rc.env.cr.Get(&createdId, sql, args...)
	newRef := ref.model.toRef(createdId)
	env.cache.copyPointer(ref, newRef)
	env.cache.scheduledInsert[ref] = newRef
}

// commit the transaction of this environment.
//
// WARNING: Do NOT call Commit on Environment instances that you
// did not create yourself with NewEnvironment. The framework will
// automatically commit the Environment.
func (env Environment) commit() {
	env.Flush()
	env.Cr().tx.Commit()
}

// rollback the transaction of this environment.
//
// WARNING: Do NOT call Rollback on Environment instances that you
// did not create yourself with NewEnvironment. Just panic instead
// for the framework to roll back automatically for you.
func (env Environment) rollback() {
	env.Cr().tx.Rollback()
}

// newEnvironment returns a new Environment with the given parameters
// in a new DB transaction.
//
// WARNING: Callers to NewEnvironment should ensure to either call Commit()
// or Rollback() on the returned Environment after operation to release
// the database connection.
func newEnvironment(uid int64, context ...types.Context) Environment {
	var ctx types.Context
	if len(context) > 0 {
		ctx = context[0]
	}
	env := Environment{
		cr:      newCursor(db),
		uid:     uid,
		context: &ctx,
		cache:   newCache(),
	}
	return env
}

// ExecuteInNewEnvironment executes the given fnct in a new Environment
// within a new transaction.
//
// This function commits the transaction if everything went right or
// rolls it back otherwise, returning an arror. Database serialization
// errors are automatically retried several times before returning an
// error if they still occur.
func ExecuteInNewEnvironment(uid int64, fnct func(Environment)) (error) {
	env := newEnvironment(uid)
	var rError error
	defer func() {
		if r := recover(); r != nil {
			env.rollback()
			if err, ok := r.(error); ok && adapters[db.DriverName()].isSerializationError(err) {
				// Transaction error
				env.retries++
				if env.retries < DBSerializationMaxRetries {
					if ExecuteInNewEnvironment(uid, fnct) == nil {
						rError = nil
						return
					}
				}
			}
			rError = logging.LogPanicData(r)
			return
		}
		env.commit()
	}()
	fnct(env)
	return rError
}

// SimulateInNewEnvironment executes the given fnct in a new Environment
// within a new transaction and rolls back the transaction at the end.
//
// This function always rolls back the transaction but returns an error
// only if fnct panicked during its execution.
func SimulateInNewEnvironment(uid int64, fnct func(Environment)) (rError error) {
	env := newEnvironment(uid)
	defer func() {
		env.rollback()
		if r := recover(); r != nil {
			rError = logging.LogPanicData(r)
			return
		}
	}()
	fnct(env)
	return
}

// Pool returns an empty RecordCollection for the given modelName
func (env Environment) Pool(modelName string) *RecordCollection {
	return newRecordCollection(env, modelName)
}
