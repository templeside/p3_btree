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

	// allocate the first leaf page
	PageId childid;
	Page* child_page;
	bufMgrIn->allocPage(&indexFile, childid, child_page);
	LeafNodeInt* child_node = reinterpret_cast<LeafNodeInt*>(child_page);
	child_node->rightSibPageNo = -1;
	for(int i = 0; i < leafOccupancy; i++){
		child_node->keyArray[i] = INT_MAX;
	}
	
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
	this->height = 2;

	//unpin
	bufMgrIn->unPinPage(&indexFile,pid,true);
	bufMgrIn->unPinPage(&indexFile,rootid,true);
	bufMgrIn->unPinPage(&indexFile,childid,true);

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
	Page* root_page;
	bufMgr->readPage(file,rootPageNum,root_page);
	NonLeafNodeInt* root_node = reinterpret_cast<NonLeafNodeInt*>(root_page);
	
	NonLeafNodeInt* current = root_node; //pointer to the current internal node looking at
	PageId child_pid;
	LeafNodeInt* leaf;
	Page* child_page;

	node_stack.push(root_node);
	pid_stack.push(rootPageNum);


	while(current->level>=1){
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

		bufMgr->readPage(file,child_pid,child_page);

		//only place to exit while loop
		if(current->level==1){
			leaf = reinterpret_cast<LeafNodeInt*>(child_page);
			break;
		}
		//update current and continue the while loop
		else{
			current = reinterpret_cast<NonLeafNodeInt*>(child_page);
		}
		node_stack.push(current);
		pid_stack.push(child_pid);
	}
	//found the leaf page to insert

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
		//clean up
		bufMgr->unPinPage(file,child_pid,true);
		while(!node_stack.empty()){
			bufMgr->unPinPage(file,pid_stack.top(),false);
			node_stack.pop();
			pid_stack.pop();
		}
	}

	//leaf does not have enough space
	else{
		PageId new_pid;
		Page* new_page;
		LeafNodeInt* new_leaf;
		bufMgr->allocPage(file,new_pid,new_page);
		new_leaf = reinterpret_cast<LeafNodeInt*>(new_page);

		//copy everything to the new array, insert at the corresponding location
		RIDKeyPair<int>* deepCopy[leafOccupancy+1];
		for (int a=0;a<leafOccupancy;a++){
			deepCopy[a] = new RIDKeyPair<int>;
			deepCopy[a]->set(leaf->ridArray[a],leaf->keyArray[a]);
		}

		//insert the new key
		int m = 0;
		while(int_key>(deepCopy[m]->key)){
			m++;
		}
		for(int b=leafOccupancy;b>m;b--){
			deepCopy[b] = deepCopy[b-1];
		}
		deepCopy[m] = new RIDKeyPair<int>;
		deepCopy[m]->set(rid,int_key);
		int half = (leafOccupancy+1)/2;

		//update the original child
		for(int c=0;c<leafOccupancy;c++){
			if(c<half){
				leaf->keyArray[c] = deepCopy[c]->key;
				leaf->ridArray[c] = deepCopy[c]->rid;
			}
			else{
				leaf->keyArray[c] = INT_MAX;
			}
		}
		
		//update the new child
		for (int c=0;c<leafOccupancy;c++){
			if(c+half<leafOccupancy+1){
				new_leaf->keyArray[c] = deepCopy[c+half]->key;
				new_leaf->ridArray[c] = deepCopy[c+half]->rid;
			}
			else{
				new_leaf->keyArray[c] = INT_MAX;
			}
		}

		//set the fields correspondingly
		new_leaf->rightSibPageNo = leaf->rightSibPageNo;
		leaf->rightSibPageNo = new_pid;
		leaf->stored = half;
		new_leaf->stored = leafOccupancy+1-half;

		//cleanup the new array created
		for(int c=0;c<leafOccupancy+1;c++){
			delete deepCopy[c];
		}

		int copy_up = new_leaf->keyArray[0];

		//clean up the two leaf pages
		bufMgr->unPinPage(file,child_pid,true);
		bufMgr->unPinPage(file,new_pid,true);

		insert_internal(copy_up,new_pid);

	}

}

void BTreeIndex::insert_internal(int key,PageId new_child_pid){
	NonLeafNodeInt* parent = node_stack.top();
	PageId parent_pid = pid_stack.top();
	if(parent->stored<nodeOccupancy){
		int m = 0;
		while(key>(parent->keyArray[m])){
			m++;
		}
		int n;
		
		for(n=parent->stored;n>m;n--){
			parent->keyArray[n] = parent->keyArray[n-1];
			parent->pageNoArray[n+1] = parent->pageNoArray[n];
		}
		parent->keyArray[m] = key;
		parent->pageNoArray[m+1] = new_child_pid;
		parent->stored++;
		//clean up
		bufMgr->unPinPage(file,parent_pid,true);
		while(!node_stack.empty()){
			bufMgr->unPinPage(file,pid_stack.top(),false);
			node_stack.pop();
			pid_stack.pop();
		}
		return;
	}
	else{
		PageId new_pid;
		Page* new_page;
		NonLeafNodeInt* new_nonleaf;
		bufMgr->allocPage(file,new_pid,new_page);
		new_nonleaf = reinterpret_cast<NonLeafNodeInt*>(new_page);

		//copy everything to the new array, insert at the corresponding location
		int keyCopy[nodeOccupancy+1];
		PageId pNoCopy[nodeOccupancy+2];
		int a;
		for(a=0;a<nodeOccupancy;a++){
			keyCopy[a] = parent->keyArray[a];
			pNoCopy[a] = parent->pageNoArray[a];
		}
		pNoCopy[a] = parent->pageNoArray[a];

		//insert the new key
		int m = 0;
		while(key>keyCopy[m]){
			m++;
		}
		for(int b=nodeOccupancy;b>m;b--){
			keyCopy[b] = keyCopy[b-1];
			pNoCopy[b+1] = pNoCopy[b];
		}
		keyCopy[m] = key;
		pNoCopy[m+1]=new_child_pid;	

		int half = nodeOccupancy/2;//the key to be push up
		//update the original child
		for(int c=0;c<nodeOccupancy;c++){
			if(c<half){
				parent->keyArray[c] = keyCopy[c];
				parent->pageNoArray[c] = pNoCopy[c];
			}
			else{
				parent->keyArray[c] = INT_MAX;
				if(c==half){
					parent->pageNoArray[c] = pNoCopy[c];
				}
			}
		}
		parent->stored = half;
		
		//update the new internal node
		for (int c=0;c<nodeOccupancy;c++){
			if(c+half+1<nodeOccupancy+1){
				new_nonleaf->keyArray[c] = keyCopy[c+half+1];
				new_nonleaf->pageNoArray[c] = pNoCopy[c+half+1];
			}
			else{
				new_nonleaf->keyArray[c] = INT_MAX;
				if(c+half+1==nodeOccupancy+1){
					new_nonleaf->pageNoArray[c] = pNoCopy[c+half+1];
				}
			}
		}
		new_nonleaf->level = parent->level;
		new_nonleaf->stored = nodeOccupancy-half;

		//check root or not
		int push_up = keyCopy[half];

		if(parent_pid==rootPageNum){
			PageId new_root_pid;
			Page* new_root_page;
			NonLeafNodeInt* new_root;
			bufMgr->allocPage(file,new_root_pid,new_root_page);
			new_root = reinterpret_cast<NonLeafNodeInt*>(new_root_page);

			new_root->keyArray[0] = push_up;
			new_root->pageNoArray[0] = parent_pid;
			new_root->pageNoArray[1] = new_pid;
			for(int a=1;a<nodeOccupancy;a++){
				new_root->keyArray[a] = INT_MAX;
				new_root->pageNoArray[a+1] = Page::INVALID_NUMBER;
			}
			new_root->level = parent->level+1;
			new_root->stored = 1;


			this->height++;
			this->rootPageNum = new_root_pid;

			//update the metapage
			PageId pid;
			Page* meta_page;
			bufMgr->readPage(file,pid,meta_page);
			IndexMetaInfo* index_meta = reinterpret_cast<IndexMetaInfo*>(meta_page);
			index_meta->rootPageNo = new_root_pid;

			//unpin
			bufMgr->unPinPage(file,parent_pid,true);
			bufMgr->unPinPage(file,new_pid,true);
			bufMgr->unPinPage(file,new_root_pid,true);
			bufMgr->unPinPage(file,pid,true);
			return;
		}
		else{
			//clean up and update next insert's parent before calling itself
			bufMgr->unPinPage(file,parent_pid,true);
			bufMgr->unPinPage(file,new_pid,true);
			node_stack.pop();
			pid_stack.pop();
			insert_internal(push_up,new_pid);
		}

	}

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
