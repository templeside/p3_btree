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
	std::stack<PageId>pid_stack;

	Page* root_page;
	bufMgr->readPage(file,rootPageNum,root_page);
	NonLeafNodeInt* root_node = reinterpret_cast<NonLeafNodeInt*>(root_page);
	NonLeafNodeInt* current = root_node;
	PageId child_pid;
	LeafNodeInt* leaf;
	Page* child_page;
	LeafNodeInt* child_node;

	stack.push(root_node);
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
		stack.push(current);
		pid_stack.push(child_pid);
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
		delete deepCopy;

		int copy_up = new_leaf->keyArray[0];

		if(current->stored<nodeOccupancy){
			int m = 0;
			while(copy_up>(current->keyArray[m])){
				m++;
			}
			for(int n=current->stored;n>m;n--){
				current->keyArray[n] = current->keyArray[n-1];
				current->pageNoArray[n] = current->pageNoArray[n-1];
			}
			current->keyArray[m] = int_key;
			current->pageNoArray[m] = new_pid;
			current->stored++;
			bufMgr->unPinPage(file,child_pid,true);
			bufMgr->unPinPage(file,new_pid,true);
			//the direct parent is dirty
			stack.pop();
			PageId cur_pid = pid_stack.top();
			pid_stack.pop();
			bufMgr->unPinPage(file,cur_pid,true);
			//the rest are not
			while(!stack.empty()){
				stack.pop();
				cur_pid = pid_stack.top();
				pid_stack.pop();
				bufMgr->unPinPage(file,cur_pid,false);
			}
		}
		else{
			bufMgr->unPinPage(file,child_pid,true);
			bufMgr->unPinPage(file,new_pid,true);			
			while(current->stored==nodeOccupancy){
				copy_up = parent_split(copy_up,current,new_pid);
				current = stack.top();
				stack.pop();
			}
		}

	}

}

int BTreeIndex::parent_split(int key,NonLeafNodeInt* current,PageId child_pid){
	PageId new_pid;
	Page* new_page;
	NonLeafNodeInt* new_nonleaf;
	bufMgr->allocPage(file,new_pid,new_page);
	new_nonleaf = reinterpret_cast<NonLeafNodeInt*>(new_page);

	//copy everything to the new array, insert at the corresponding location
	int keyCopy[nodeOccupancy+1];
	PageId pNoCopy[nodeOccupancy+2];
	int a=0;
	for (;a<nodeOccupancy;a++){
		keyCopy[a] = current->keyArray[a];
		pNoCopy[a] = current->pageNoArray[a];
	}
	pNoCopy[a+1] = current->pageNoArray[a+1];

	//insert the new key
	int m = 0;
	while(key>keyCopy[m]){
		m++;
	}
	for(int b=nodeOccupancy;b>m;b--){
		keyCopy[b] = keyCopy[b-1];
	}
	for(int b=nodeOccupancy+1;b>m+1;b--){
		pNoCopy[b] = pNoCopy[b-1];
	}
	keyCopy[m] = key;
	pNoCopy[m+1]=child_pid;

	int half = (nodeOccupancy+1)/2;//the key to be push up
	//update the original child
	for(int c=0;c<nodeOccupancy;c++){
		if(c<half){
			current->keyArray[c] = keyCopy[c];
			current->pageNoArray[c] = pNoCopy[c];
		}
		else{
			current->keyArray[c] = INT_MAX;
		}
	}

	
	//update the new internal node
	for (int c=0;c<nodeOccupancy;c++){
		if(c+half+1<nodeOccupancy+1){
			new_nonleaf->keyArray[c] = keyCopy[c+half+1];
			new_nonleaf->pageNoArray[c] = pNoCopy[c+half+1];
		}
		else{
			new_nonleaf->keyArray[c] = INT_MAX;
		}
	}

	int push_up = keyCopy[half];
	//set the fields correspondingly
	current->stored = half;
	new_nonleaf->stored = nodeOccupancy+1-half-1;

	// //cleanup the new array created
	// for(int c=0;c<leafOccupancy+1;c++){
	// 	delete deepCopy[c];
	// }
	// delete deepCopy;
	// int copy_up = new_leaf->keyArray[0];
	return push_up;
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
