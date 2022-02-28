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
#include <climits>
#include <stack>


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
	IndexMetaInfo* index_meta = reinterpret_cast<IndexMetaInfo*>(meta_page);
	strcpy(index_meta->relationName,relationName.c_str());
	index_meta->attrByteOffset = attrByteOffset;
	index_meta->attrType = attrType;
	
	//allocate root page
	PageId rootid;
	Page* root_page;
	bufMgrIn->allocPage(&indexFile,rootid,root_page);
	NonLeafNodeInt* root_node=reinterpret_cast<NonLeafNodeInt*>(root_page);
	for(int i=0;i<INTARRAYNONLEAFSIZE;i++){
		root_node->keyArray[i] = INT_MAX;
		root_node->pageNoArray[i] = Page::INVALID_NUMBER;
	}
	root_node->level = 1;
	


	//fill in fields of btree
	index_meta->rootPageNo = rootid;
	this->file = &indexFile;
	this->bufMgr = bufMgrIn;
	this->headerPageNum = pid;
	this->rootPageNum = rootid;
	this->attributeType = attrType;
	this->attrByteOffset = attrByteOffset;
	this->leafOccupancy = INTARRAYLEAFSIZE;
	this->nodeOccupancy = INTARRAYNONLEAFSIZE;

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
			int* key = reinterpret_cast<int*>(&data + attrByteOffset);
			insertEntry(key,rid);
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

	int int_key = *(int*)key;
	std::stack<NonLeafNodeInt*>stack;

	Page* root_page;
	bufMgr->readPage(file,rootPageNum,root_page);
	NonLeafNodeInt* root_node = reinterpret_cast<NonLeafNodeInt*>(root_page);
	NonLeafNodeInt* current = root_node;
	LeafNodeInt* leaf;
	Page* child_page;
	LeafNodeInt* child_node;

	while(current->level!=1){
		PageId child_pid;
		stack.push(current);
		int i;
		//find the child node to proceed
		for(i=0;i<nodeOccupancy;i++){
			if (int_key<root_node->keyArray[i]){
				child_pid = root_node->pageNoArray[i];
				break;
			}
			if(i==nodeOccupancy-1){
				i++;
				child_pid = root_node->pageNoArray[i];
			}
		}
		//place 1 to exit while loop
		if (child_pid==Page::INVALID_NUMBER){
			bufMgr->allocPage(file,child_pid,child_page);
			leaf = reinterpret_cast<LeafNodeInt*>(child_page);
			for(int j=0;j<leafOccupancy;j++){
				leaf->keyArray[j] = INT_MAX;
			}
			current->pageNoArray[i] = child_pid;
			//set the right sibling pageid for its left sibling
			//the condition may need revision???
			if((i!=0) ||(current!=root_node)){
				PageId left_pid = current->pageNoArray[i-1];
				Page* left_page;
				bufMgr->readPage(file,left_pid,left_page);
				LeafNodeInt* left_node = reinterpret_cast<LeafNodeInt*>(left_page);
				left_node->rightSibPageNo = child_pid;
				bufMgr->unPinPage(file,left_pid,true);
			}
			break;
		}

		bufMgr->readPage(file,child_pid,child_page);

		//place 2 to exit while loop
		if(current->level==1){
			leaf = reinterpret_cast<LeafNodeInt*>(child_page);
			break;
		}
		//update current and continue the while loop
		else{
			current = reinterpret_cast<NonLeafNodeInt*>(child_page);
		}
	}
	//have the leaf page

	//leaf has enough space
	if(leaf->stored<leafOccupancy){
		int m = 0;
		while(int_key>(leaf->keyArray[m])){
			m++;
		}
		for(int n=leaf->stored;n>m;n--){
			leaf->keyArray[n] = leaf->keyArray[n-1];
			leaf->ridArray[n] = leaf->ridArray[n-1];
		}
		leaf->keyArray[m] = int_key;
		leaf->ridArray[m] = rid;
		leaf->stored++;
		//TODO: clean up
	}
	//leaf does not have enough space
	else{
		PageId new_pid;
		Page* new_page;
		LeafNodeInt* new_leaf;
		bufMgr->allocPage(file,new_pid,new_page);
		new_leaf = reinterpret_cast<LeafNodeInt*>(new_page);
		int half = (leafOccupancy+1)/2-1; //index of last element on the left
		int m = 0;
		while(int_key>(leaf->keyArray[m])){
			m++;
		}
		//copy everything to the new array, insert at the corresponding location
		//scopy first half and second half to the two pages
		//do copy up and the setting of rightsibling field correspondingly
		RIDKeyPair* deepCopy[];
		if(m<=half){
			for(int n=half;n<leaf->stored;n++){
				new_leaf->keyArray[n-half] = leaf->keyArray[n];
				new_leaf->ridArray[n-half] = leaf->ridArray[n];
			}

		}
	}

	



//unpin eveything in the stack at the end
}

//void BTreeIndex::split(int*key,const RecordId rid, )

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
