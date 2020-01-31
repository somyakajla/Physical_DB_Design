/**
 * @file heap_storage.cpp - implementation of:
 * SlottedPage
 * HeapFile
 * HeapTable
 *
 * @author Kevin Lundeen
 * @see "Seattle University, CPSC5300, Summer 2018"
 */
#include <stdio.h>
#include <stdlib.h>
#include <memory.h>
#include "heap_storage.h"
using namespace std;

typedef uint16_t u16;

SlottedPage::SlottedPage(Dbt &block, BlockID block_id, bool is_new) : DbBlock(block, block_id, is_new) {
	if (is_new) {
		this->num_records = 0;
		this->end_free = DbBlock::BLOCK_SZ - 1;
		put_header();
	} else {
		get_header(this->num_records, this->end_free);
	}
}

// Add a new record to the block. Return its id.
RecordID SlottedPage::add(const Dbt* data) throw(DbBlockNoRoomError) {
	if (!has_room((u16)data->get_size()))
		throw DbBlockNoRoomError("not enough room for new record");
	u16 id = ++this->num_records;
	u16 size = (u16) data->get_size();
	this->end_free -= size;
	u16 loc = this->end_free + 1U;
	put_header();
	put_header(id, size, loc);
	memcpy(this->address(loc), data->get_data(), size);
	return id;
}

// Get a record from the block. Return None if it has been deleted.
Dbt* SlottedPage::get(RecordID record_id) const {
	u16 size, loc;
    get_header(size, loc, record_id);
    if (loc == 0)
        return nullptr;  // this is just a tombstone, record has been deleted
    return new Dbt(this->address(loc), size);
}

// Replace the record with the given data. Raises DbBlockNoRoomError if it won't fit.
void SlottedPage::put(RecordID record_id, const Dbt &data) throw(DbBlockNoRoomError) {
	u16 size, loc;
    get_header(size, loc, record_id);
    u16 new_size = (u16) data.get_size();
    if (new_size > size) {
        u16 extra = new_size - size;
        if (!has_room(extra))
    		throw DbBlockNoRoomError("not enough room for enlarged record");
		slide(loc, loc - extra);
		memcpy(this->address(loc-extra), data.get_data(), new_size);
	} else {
		memcpy(this->address(loc), data.get_data(), new_size);
        slide(loc+new_size, loc+size);
	}
    get_header(size, loc, record_id);
    put_header(record_id, new_size, loc);
}

// Mark the given id as deleted by changing its size to zero and its location to 0.
// Compact the rest of the data in the block. But keep the record ids the same for everyone.
void SlottedPage::del(RecordID record_id) {
	u16 size, loc;
    get_header(size, loc, record_id);
    put_header(record_id, 0, 0);
    slide(loc, loc+size);
}

// Sequence of all non-deleted record IDs.
RecordIDs* SlottedPage::ids(void) const {
	RecordIDs* vec = new RecordIDs();
	u16 size, loc;
	for (RecordID record_id = 1; record_id <= this->num_records; record_id++) {
	    get_header(size, loc, record_id);
	    if (loc != 0)
	    	vec->push_back(record_id);
	}
	return vec;
}

// Get the size and offset for given id. For id of zero, it is the block header.
void SlottedPage::get_header(u16 &size, u16 &loc, RecordID id) const {
	size = get_n((u16) 4*id);
	loc = get_n((u16)(4*id + 2));
}

// Store the size and offset for given id. For id of zero, store the block header.
void SlottedPage::put_header(RecordID id, u16 size, u16 loc) {
	if (id == 0) {
		size = this->num_records;
		loc = this->end_free;
	}
	put_n((u16)4*id, size);
	put_n((u16)(4*id + 2), loc);
}

// Calculate if we have room to store a record with given size. The size should include the 4 bytes
// for the header, too, if this is an add.
bool SlottedPage::has_room(u16 size) const {
	u16 available = this->end_free - (u16)(4*(this->num_records+1));
	return size <= available;
}

// If start < end, then remove data from offset start up to but not including offset end by sliding data
// that is to the left of start to the right. If start > end, then make room for extra data from end to start
// by sliding data that is to the left of start to the left.
// Also fix up any record headers whose data has slid. Assumes there is enough room if it is a left
// shift (end < start).
void SlottedPage::slide(u16 start, u16 end) {
    int shift = end - start;
    if (shift == 0)
        return;

    // slide data
    void *to = this->address((u16)(this->end_free + 1 + shift));
    void *from = this->address((u16)(this->end_free + 1));
    int bytes = start - (this->end_free + 1U);
    char temp[bytes];
    memcpy(temp, from, bytes);
    memcpy(to, temp, bytes);

    // fix up headers
    RecordIDs* record_ids = ids();
	for (auto const& record_id : *record_ids) {
		u16 size, loc;
		get_header(size, loc, record_id);
		if (loc <= start) {
			loc += shift;
			put_header(record_id, size, loc);
		}
	}
    delete record_ids;
    this->end_free += shift;
    put_header();
}

// Get 2-byte integer at given offset in block.
u16 SlottedPage::get_n(u16 offset) const {
	return *(u16*)this->address(offset);
}

// Put a 2-byte integer at given offset in block.
void SlottedPage::put_n(u16 offset, u16 n) {
	*(u16*)this->address(offset) = n;
}

// Get a void* pointer into the data block.
void* SlottedPage::address(u16 offset) const {
	return (void*)((char*)this->block.get_data() + offset);
}


/*
 * *******************
 * HeapFile class
 * *******************
 */

HeapFile::HeapFile(string name) : DbFile(name), dbfilename(""), last(0), closed(true), db(_DB_ENV, 0) {
	this->dbfilename = this->name + ".db";
}

// Create physical file.
void HeapFile::create(void) {
	db_open(DB_CREATE|DB_EXCL);
	SlottedPage *page = get_new(); // force one page to exist
	delete page;
}

// Delete the physical file.
void HeapFile::drop(void) {
	close();
	Db db(_DB_ENV, 0);
	db.remove(this->dbfilename.c_str(), nullptr, 0);
}

// Open physical file.
void HeapFile::open(void) {
    db_open();
}

// Close the physical file.
void HeapFile::close(void) {
	this->db.close(0);
	this->closed = true;
}

// Allocate a new block for the database file.
// Returns the new empty DbBlock that is managing the records in this block and its block id.
SlottedPage* HeapFile::get_new(void) {
	char block[DbBlock::BLOCK_SZ];
	memset(block, 0, sizeof(block));
	Dbt data(block, sizeof(block));

	int block_id = ++this->last;
	Dbt key(&block_id, sizeof(block_id));

	// write out an empty block and read it back in so Berkeley DB is managing the memory
	SlottedPage* page = new SlottedPage(data, this->last, true);
	this->db.put(nullptr, &key, &data, 0); // write it out with initialization done to it
	delete page;
	this->db.get(nullptr, &key, &data, 0);
	return new SlottedPage(data, this->last);
}

// Get a block from the database file.
SlottedPage* HeapFile::get(BlockID block_id) {
	Dbt key(&block_id, sizeof(block_id));
	Dbt data;
	this->db.get(nullptr, &key, &data, 0);
	return new SlottedPage(data, block_id, false);
}

// Write a block back to the database file.
void HeapFile::put(DbBlock* block) {
	int block_id = block->get_block_id();
	Dbt key(&block_id, sizeof(block_id));
	this->db.put(nullptr, &key, block->get_block(), 0);
}

// Sequence of all block ids.
BlockIDs* HeapFile::block_ids() const {
	BlockIDs* vec = new BlockIDs();
	for (BlockID block_id = 1; block_id <= this->last; block_id++)
		vec->push_back(block_id);
	return vec;
}

uint32_t HeapFile::get_block_count() {
	DB_BTREE_STAT* stat;
	this->db.stat(nullptr, &stat, DB_FAST_STAT);
	return stat->bt_ndata;
}

// Wrapper for Berkeley DB open, which does both open and creation.
void HeapFile::db_open(uint flags) {
    if (!this->closed)
        return;
    this->db.set_re_len(DbBlock::BLOCK_SZ); // record length - will be ignored if file already exists
    this->db.open(nullptr, this->dbfilename.c_str(), nullptr, DB_RECNO, flags, 0644);

	this->last = flags ? 0 : get_block_count();
    this->closed = false;
}


/*
 * *******************
 * HeapTable class
 * *******************
 */

HeapTable::HeapTable(Identifier table_name, ColumnNames column_names, ColumnAttributes column_attributes ) :
		DbRelation(table_name, column_names, column_attributes), file(table_name) {
}

// Execute: CREATE TABLE <table_name> ( <columns> )
// Is not responsible for metadata storage or validation.
void HeapTable::create() {
	file.create();
}

// Execute: CREATE TABLE IF NOT EXISTS <table_name> ( <columns> )
// Is not responsible for metadata storage or validation.
void HeapTable::create_if_not_exists() {
	try {
		open();
	} catch (DbException& e) {
		create();
	}
}

// Execute: DROP TABLE <table_name>
void HeapTable::drop() {
	file.drop();
}

// Open existing table. Enables: insert, update, delete, select, project
void HeapTable::open() {
	file.open();
}

// Closes the table. Disables: insert, update, delete, select, project
void HeapTable::close() {
	file.close();
}

// Expect row to be a dictionary with column name keys.
// Execute: INSERT INTO <table_name> (<row_keys>) VALUES (<row_values>)
// Return the handle of the inserted row.
Handle HeapTable::insert(const ValueDict* row) {
    open();
    ValueDict* full_row = validate(row);
    Handle handle = append(full_row);
    delete full_row;
    return handle;
}

// Expect new_values to be a dictionary with column name keys.
// Conceptually, execute: UPDATE INTO <table_name> SET <new_values> WHERE <handle>
// where handle is sufficient to identify one specific record (e.g., returned from an insert
// or select).
void HeapTable::update(const Handle handle, const ValueDict* new_values) {
	throw DbRelationError("Not implemented");
}

// Conceptually, execute: DELETE FROM <table_name> WHERE <handle>
// where handle is sufficient to identify one specific record (e.g., returned from an insert
// or select).
void HeapTable::del(const Handle handle) {
	open();
	BlockID block_id = handle.first;
	RecordID record_id = handle.second;
	SlottedPage* block = this->file.get(block_id);
	block->del(record_id);
	this->file.put(block);
	delete block;
}

// Conceptually, execute: SELECT <handle> FROM <table_name> WHERE 1
// Returns a list of handles for qualifying rows.
Handles* HeapTable::select() {
	return select(nullptr);
}

// Conceptually, execute: SELECT <handle> FROM <table_name> WHERE <where>
// Returns a list of handles for qualifying rows.
Handles* HeapTable::select(const ValueDict* where) {
	open();
	Handles* handles = new Handles();
	BlockIDs* block_ids = file.block_ids();
    for (auto const& block_id: *block_ids) {
    	SlottedPage* block = file.get(block_id);
    	RecordIDs* record_ids = block->ids();
    	for (auto const& record_id: *record_ids) {
			Handle handle(block_id, record_id);
			if (selected(handle, where))
    			handles->push_back(Handle(block_id, record_id));
		}
    	delete record_ids;
    	delete block;
    }
    delete block_ids;
	return handles;
}

// Return a sequence of all values for handle.
ValueDict* HeapTable::project(Handle handle) {
	return project(handle, &this->column_names);
}

// Return a sequence of values for handle given by column_names.
ValueDict* HeapTable::project(Handle handle, const ColumnNames* column_names) {
	BlockID block_id = handle.first;
	RecordID record_id = handle.second;
    SlottedPage* block = file.get(block_id);
    Dbt* data = block->get(record_id);
    ValueDict* row = unmarshal(data);
    delete data;
    delete block;
    if (column_names->empty())
    	return row;
    ValueDict* result = new ValueDict();
    for (auto const& column_name: *column_names) {
		if (row->find(column_name) == row->end())
			throw DbRelationError("table does not have column named '" + column_name + "'");
    	(*result)[column_name] = (*row)[column_name];
	}
	delete row;
    return result;
}

// Check if the given row is acceptable to insert. Raise ValueError if not.
// Otherwise return the full row dictionary.
ValueDict* HeapTable::validate(const ValueDict* row) const {
    ValueDict* full_row = new ValueDict();
    for (auto const& column_name: this->column_names) {
    	Value value;
    	ValueDict::const_iterator column = row->find(column_name);
    	if (column == row->end())
    		throw DbRelationError("don't know how to handle NULLs, defaults, etc. yet");
    	else
    		value = column->second;
    	(*full_row)[column_name] = value;
    }
    return full_row;
}

// Assumes row is fully fleshed-out. Appends a record to the file.
Handle HeapTable::append(const ValueDict* row) {
    Dbt* data = marshal(row);
    SlottedPage* block = this->file.get(this->file.get_last_block_id());
    RecordID record_id;
    try {
        record_id = block->add(data);
    } catch (DbBlockNoRoomError& e) {
    	// need a new block
    	block = this->file.get_new();
    	record_id = block->add(data);
    }
    this->file.put(block);
	delete block;
    delete[] (char*)data->get_data();
    delete data;
    return Handle(this->file.get_last_block_id(), record_id);
}

// return the bits to go into the file
// caller responsible for freeing the returned Dbt and its enclosed ret->get_data().
Dbt* HeapTable::marshal(const ValueDict* row) const {
	char *bytes = new char[DbBlock::BLOCK_SZ]; // more than we need (we insist that one row fits into DbBlock::BLOCK_SZ)
    uint offset = 0;
    uint col_num = 0;
    for (auto const& column_name: this->column_names) {
    	ColumnAttribute ca = this->column_attributes[col_num++];
    	ValueDict::const_iterator column = row->find(column_name);
		Value value = column->second;

		if (ca.get_data_type() == ColumnAttribute::DataType::INT) {
			if (offset + 4 > DbBlock::BLOCK_SZ - 4)
				throw DbRelationError("row too big to marshal");
			*(int32_t*) (bytes + offset) = value.n;
			offset += sizeof(int32_t);
		} else if (ca.get_data_type() == ColumnAttribute::DataType::TEXT) {
			u_long size = value.s.length();
			if (size > UINT16_MAX)
				throw DbRelationError("text field too long to marshal");
			if (offset + 2 + size > DbBlock::BLOCK_SZ)
				throw DbRelationError("row too big to marshal");
			*(u16*) (bytes + offset) = size;
			offset += sizeof(u16);
			memcpy(bytes+offset, value.s.c_str(), size); // assume ascii for now
			offset += size;
		} else {
			throw DbRelationError("Only know how to marshal INT and TEXT");
		}
	}
	char *right_size_bytes = new char[offset];
	memcpy(right_size_bytes, bytes, offset);
	delete[] bytes;
	Dbt *data = new Dbt(right_size_bytes, offset);
	return data;
}

ValueDict* HeapTable::unmarshal(Dbt* data) const {
    ValueDict *row = new ValueDict();
    Value value;
    char *bytes = (char*)data->get_data();
    uint offset = 0;
    uint col_num = 0;
    for (auto const& column_name: this->column_names) {
    	ColumnAttribute ca = this->column_attributes[col_num++];
		value.data_type = ca.get_data_type();
    	if (ca.get_data_type() == ColumnAttribute::DataType::INT) {
    		value.n = *(int32_t*)(bytes + offset);
    		offset += sizeof(int32_t);
    	} else if (ca.get_data_type() == ColumnAttribute::DataType::TEXT) {
    		u16 size = *(u16*)(bytes + offset);
    		offset += sizeof(u16);
    		char buffer[DbBlock::BLOCK_SZ];
    		memcpy(buffer, bytes+offset, size);
    		buffer[size] = '\0';
    		value.s = string(buffer);  // assume ascii for now
            offset += size;
    	} else {
            throw DbRelationError("Only know how to unmarshal INT and TEXT");
    	}
		(*row)[column_name] = value;
    }
    return row;
}

// See if the row at the given handle satisfies the given where clause
bool HeapTable::selected(Handle handle, const ValueDict* where) {
	if (where == nullptr)
		return true;
	ValueDict* row = this->project(handle, where);
	return *row == *where;
}

void test_set_row(ValueDict &row, int a, string b) {
	row["a"] = Value(a);
	row["b"] = Value(b);
}

bool test_compare(DbRelation &table, Handle handle, int a, string b) {
	ValueDict *result = table.project(handle);
	Value value = (*result)["a"];
	if (value.n != a) {
		delete result;
		return false;
	}
	value = (*result)["b"];
	delete result;
	return !(value.s != b);
}

// test function -- returns true if all tests pass
bool test_heap_storage() {
	ColumnNames column_names;
	column_names.push_back("a");
	column_names.push_back("b");
	ColumnAttributes column_attributes;
	ColumnAttribute ca(ColumnAttribute::INT);
	column_attributes.push_back(ca);
	ca.set_data_type(ColumnAttribute::TEXT);
	column_attributes.push_back(ca);
    HeapTable table1("_test_create_drop_cpp", column_names, column_attributes);
	cout << "test_heap_storage: " << endl;
    table1.create();
    cout << "create ok" << endl;
    table1.drop();  // drop makes the object unusable because of BerkeleyDB restriction -- maybe want to fix this some day
    cout << "drop ok" << endl;
    
	HeapTable table("_test_data_cpp", column_names, column_attributes);
    table.create_if_not_exists();
    cout << "create_if_not_exsts ok" << endl;

    ValueDict row;
	string b = "alkjsl;kj; as;lkj;alskjf;laks df;alsdkjfa;lsdkfj ;alsdfkjads;lfkj a;sldfkj a;sdlfjk a";
	test_set_row(row, -1, b);
    table.insert(&row);
    cout << "insert ok" << endl;

    Handles* handles = table.select();
    if (!test_compare(table, (*handles)[0], -1, b))
        return false;
    cout << "select/project ok " << handles->size() << endl;
	delete handles;

    Handle last_handle;
    for (int i = 0; i < 1000; i++) {
        test_set_row(row, i, b);
        last_handle = table.insert(&row);
    }
    handles = table.select();
    if (handles->size() != 1001)
        return false;
    int i = -1;
    for (auto const& handle: *handles)
        if (!test_compare(table, handle, i++, b))
            return false;
    cout << "many inserts/select/projects ok" << endl;
	delete handles;

    table.del(last_handle);
    handles = table.select();
    if (handles->size() != 1000)
        return false;
    i = -1;
    for (auto const& handle: *handles)
        if (!test_compare(table, handle, i++, b))
            return false;
    cout << "del ok" << endl;
 
    table.drop();
	delete handles;
    return true;
}
