/**
 * @author See Contributors.txt for code contributors and overview of BadgerDB.
 *
 * @section LICENSE
 * Copyright (c) 2012 Database Group, Computer Sciences Department, University of Wisconsin-Madison.
 */

#include "btree.h"
#include "filescan.h"
#include "exceptions/bad_index_info_exception.h"
#include "exceptions/bad_opcodes_exception.h"
#include "exceptions/bad_scanrange_exception.h"
#include "exceptions/no_such_key_found_exception.h"
#include "exceptions/scan_not_initialized_exception.h"
#include "exceptions/index_scan_completed_exception.h"
#include "exceptions/file_not_found_exception.h"
#include "exceptions/end_of_file_exception.h"


//#define DEBUG

namespace badgerdb
{

// -----------------------------------------------------------------------------
// BTreeIndex::BTreeIndex -- Constructor
// -----------------------------------------------------------------------------

BTreeIndex::BTreeIndex(const std::string & relationName,
		std::string & outIndexName,
		BufMgr *bufMgrIn,
		const int attrByteOffset,
		const Datatype attrType)
{	
	std::ostringstream idxStr;
	idxStr << relationName << '.' << attrByteOffset;
	outIndexName = idxStr.str();
	try{
		BlobFile::exists(outIndexName);
		BlobFile indexFile = BlobFile(outIndexName,false);
		return;
	}
	catch (FileNotFoundException &e){
	}
	BlobFile indexFile = BlobFile(outIndexName,true);
	

	//allocate meta page
	PageId pid;
	Page* meta_page;
	bufMgrIn->allocPage(&indexFile,pid,meta_page);
	IndexMetaInfo* index_meta = reinterpret_cast<IndexMetaInfo*>(&meta_page);
	strcpy(index_meta->relationName,relationName.c_str());
	index_meta->attrByteOffset = attrByteOffset;
	index_meta->attrType = attrType;
	
	//allocate root page
	PageId rootid;
	Page* root_page;
	bufMgrIn->allocPage(&indexFile,rootid,root_page);
	index_meta->rootPageNo = rootid;

	//fill in fields of btree
	this->file = &indexFile;
	this->bufMgr = bufMgrIn;
	this->headerPageNum = pid;
	this->rootPageNum = rootid;
	this->attributeType = attrType;
	this->attrByteOffset = attrByteOffset;

	//unpin
	bufMgrIn->unPinPage(&indexFile,pid,true);
	bufMgrIn->unPinPage(&indexFile,rootid,true);

	//insert entries from the relation
	FileScan fc = FileScan(relationName,bufMgrIn);
	RecordId rid;
	try{
		while (true){
			fc.scanNext(rid);
			std::string data = fc.getRecord();
			char* key;
			strncpy(key,&data[attrByteOffset],sizeof(int));
			insertEntry((void*)key,rid);
		}
	}catch(EndOfFileException &e){
	}

}


// -----------------------------------------------------------------------------
// BTreeIndex::~BTreeIndex -- destructor
// -----------------------------------------------------------------------------

BTreeIndex::~BTreeIndex()
{
}

// -----------------------------------------------------------------------------
// BTreeIndex::insertEntry
// -----------------------------------------------------------------------------

void BTreeIndex::insertEntry(const void *key, const RecordId rid) 
{

}

// -----------------------------------------------------------------------------
// BTreeIndex::startScan
// -----------------------------------------------------------------------------

void BTreeIndex::startScan(const void* lowValParm,
				   const Operator lowOpParm,
				   const void* highValParm,
				   const Operator highOpParm)
{

}

// -----------------------------------------------------------------------------
// BTreeIndex::scanNext
// -----------------------------------------------------------------------------

void BTreeIndex::scanNext(RecordId& outRid) 
{

}

// -----------------------------------------------------------------------------
// BTreeIndex::endScan
// -----------------------------------------------------------------------------
//
void BTreeIndex::endScan() 
{

}

}
