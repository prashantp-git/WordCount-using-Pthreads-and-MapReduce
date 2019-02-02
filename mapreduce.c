#include <assert.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <pthread.h>
#include "mapreduce.h"

#define NUM_PARTITIONS 2

typedef struct list {
	char *val;
	struct list *next;
} List;

typedef struct key_valList {
	char *key;
	struct key_valList *next;
	List *valList_head;
	List *valList_curr_mHead;
	List *valList_curr_rHead;
} Key_valList;

typedef struct mapperPartition {
	int startIdx;
	int endIdx;
	Mapper map;
	char **argv;
} Mapper_Partition;

typedef struct reducerPartition {
	int startIdx;
	int endIdx;
	Reducer reduce;
} Reducer_Partition;

pthread_mutex_t data_mutex;
Key_valList data[NUM_PARTITIONS];
Key_valList curr_partn_data[NUM_PARTITIONS];

void Map(char *file_name) {
	FILE *fp = fopen(file_name, "r");
	assert(fp != NULL);
	char *line = NULL;
	size_t size = 0;
	while (getline(&line, &size, fp) != -1) {
		char *token, *dummy = line;
		while ((token = strsep(&dummy, " \t\n\r")) != NULL) {
			MR_Emit(token, "1");
		}
	}
	free(line);
	fclose(fp);
}

void Reduce(char *key, Getter get_next, int partition_number) {
	int count = 0;
	char *value;
	while ((value = get_next(key, partition_number)) != NULL)
		count++;
	printf("%s : %d\n", key, count);
}

char *GetNext(char *key, int pNum) {
	/*if (data[pNum].valList_curr_rHead == NULL) {
		data[pNum].valList_curr_rHead = data[pNum].valList_head;
	}
	if (data[pNum].valList_curr_rHead->val == NULL) {
		free(data[pNum].valList_curr_rHead);
		return NULL;
	}
	char* val = data[pNum].valList_curr_rHead->val;
	data[pNum].valList_curr_mHead = data[pNum].valList_curr_rHead;
	if (data[pNum].valList_curr_rHead->next == NULL) {
		data[pNum].valList_curr_rHead->val = NULL;
	} else {
		data[pNum].valList_curr_rHead =
				data[pNum].valList_curr_rHead->next;
		free(data[pNum].valList_curr_mHead);
	}
	return val;*/

	/********* CORRECT BELOW CODE **************/	
	Key_valList *curr_pData = &curr_partn_data[pNum];
	//printf("curr_pData %s\n",curr_pData->key);
	if(curr_pData->key==NULL || strcmp(curr_pData->key,key)!=0){
		int key_found = 0;
		Key_valList *temp = &data[pNum];		
		while(temp->next!=NULL){
			if(strcmp(temp->key,key)==0){
				key_found = 1;
				break;
			}else{
				temp = temp->next;
			}			
		}
		if(key_found || temp->next==NULL){
			curr_pData = temp;
		}
	}
	//return next value for curr_pData->key
	if (curr_pData->valList_curr_rHead == NULL) {
		curr_pData->valList_curr_rHead = curr_pData->valList_head;
	}
	if (curr_pData->valList_curr_rHead->val == NULL) {
		free(curr_pData->valList_curr_rHead);
		return NULL;
	}
	char* val = curr_pData->valList_curr_rHead->val;
	curr_pData->valList_curr_mHead = curr_pData->valList_curr_rHead;
	if (curr_pData->valList_curr_rHead->next == NULL) {
		curr_pData->valList_curr_rHead->val = NULL;
	} else {
		curr_pData->valList_curr_rHead =
				curr_pData->valList_curr_rHead->next;
		free(curr_pData->valList_curr_mHead);
	}
	return val;
	/*********** CORRECT ABOVE CODE *************/
}

void *partitionAndMap(void* arg) {
	Mapper_Partition *m = (Mapper_Partition*) arg;
	int i;
	for (i = m->startIdx; i < m->endIdx; i++) {
		m->map(m->argv[i]);
	}
	return NULL;
}

void *partitionAndReduce(void* arg) {
	Reducer_Partition *w = (Reducer_Partition *) arg;
	int pNum;
	//printf("In reducer--> w->startIdx: %d   w->endIdx: %d\n",w->startIdx,w->endIdx);
	for (pNum = w->startIdx; pNum < w->endIdx; pNum++) {
		Key_valList *temp = &data[pNum];
		//printf("Going for pNum %d with starting key %s\n",pNum,temp->key);
		while(temp->next!=NULL){
			w->reduce(temp->key, GetNext, pNum);
			temp = temp->next;
		}
		if(temp->next==NULL){
			w->reduce(temp->key, GetNext, pNum);
		}
	}
	return NULL;
}

void MR_Emit(char *key, char *value) {
	unsigned long pNum = MR_DefaultHashPartition(key, NUM_PARTITIONS);
	pthread_mutex_lock(&data_mutex);
	int key_found = 0;
	/*
	data[pNum].key = (char *) malloc(strlen(key));
	strcpy(data[pNum].key, key);
	*/
	Key_valList *temp = &data[pNum];
	//printf("PNum: %lu , Key: %s , Address: %p\n",pNum,temp->key,(void*)temp);
	//printf("Pointing at %lu  Incoming Key: %s  ",pNum,key);
	while(temp->next!=NULL){
		if(strcmp(temp->key,key)==0){
			//printf("Found: %s in pNum: %lu\n",temp->key, pNum);
			key_found = 1;
			break;
		}else{
			temp = temp->next;
		}			
	}
	if(!key_found && temp->key!=NULL && strcmp(temp->key,key)==0){
		key_found = 1;
		//printf("Found: %s in pNum: %lu\n",temp->key, pNum);
	}
	if(!key_found){
		Key_valList *new_data = (Key_valList *) malloc(sizeof(Key_valList));
		new_data->key = strdup(key);
		if(temp->key!=NULL){
			temp->next = new_data; 
			temp = new_data;
		}
		else{
			temp->key = new_data->key;
			temp->next = new_data->next;
		}
		//printf("Added: %s in pNum: %lu\n",temp->key,pNum);
	}

	/*List *new_node = (List *) malloc(sizeof(List));
	new_node->val = (char *) malloc(strlen(value));
	strcpy(new_node->val, value);

	if (data[pNum].valList_curr_mHead == NULL) {
		data[pNum].valList_curr_mHead = (List *) malloc(sizeof(List));
	}
	data[pNum].valList_curr_mHead->next = new_node;
	data[pNum].valList_curr_mHead = new_node;
	if (data[pNum].valList_head == NULL) {
		data[pNum].valList_head = (List *) malloc(sizeof(List));
		data[pNum].valList_head = new_node;
		data[pNum].valList_curr_mHead->next = NULL;
	}*/

	List *new_node = (List *) malloc(sizeof(List));
	new_node->val = strdup(value);
	if (temp->valList_head == NULL) {
		temp->valList_head = new_node;
		temp->valList_curr_mHead = new_node;
	}else{
		temp->valList_curr_mHead->next = new_node;
		temp->valList_curr_mHead = new_node;
	}
	//printf("key: %s  List_head: %p  List_curr_mHead: %p\n",key,(void*)temp->valList_head,(void*)temp->valList_curr_mHead);
	pthread_mutex_unlock(&data_mutex);
}

unsigned long MR_DefaultHashPartition(char *key, int num_partitions) {
	unsigned long hash = 5381;
	int c;
	while ((c = *key++) != '\0')
		hash = hash * 33 + c;
	return hash % num_partitions;
}

void MR_Run(int argc, char *argv[], Mapper map, int num_mappers, Reducer reduce,
		int num_reducers, Partitioner partition) {
	int i, num_files = argc - 1;
	pthread_attr_t attr;
	pthread_mutex_init(&data_mutex, NULL);
	pthread_attr_init(&attr);
	num_mappers = num_files > num_mappers ? num_mappers : num_files;
	num_reducers = NUM_PARTITIONS < num_reducers ? NUM_PARTITIONS : num_reducers;
	pthread_t mapThreads[num_mappers];
	pthread_t reduceThreads[num_reducers];

	// Map Phase
	Mapper_Partition m[num_mappers];
	size_t startIdx, endIdx;
	size_t blockSize =
			num_files > num_mappers ?
					(num_files + num_mappers - 1) / num_mappers : 1;
	startIdx = 1;
	endIdx = startIdx + blockSize;
	for (i = 0; i < num_mappers; i++) {
		m[i].startIdx = startIdx;
		m[i].endIdx = endIdx;
		m[i].map = map;
		m[i].argv = argv;
		pthread_create(&mapThreads[i], NULL, partitionAndMap, (void*) &m[i]);
		startIdx = endIdx;
		endIdx = (
				endIdx + blockSize < num_files ?
						endIdx + blockSize : num_files + 1);
	}
	for (i = 0; i < num_mappers; i++) {
		pthread_join(mapThreads[i], NULL);
	}

	// ReducePhase
	Reducer_Partition w[num_reducers];
	blockSize = NUM_PARTITIONS / num_reducers;
	startIdx = 0;
	endIdx = startIdx + blockSize;
	for (i = 0; i < num_reducers; i++) {
		w[i].startIdx = startIdx;
		w[i].endIdx = endIdx;
		w[i].reduce = reduce;
		pthread_create(&reduceThreads[i], NULL, partitionAndReduce,
				(void*) &w[i]);
		startIdx = endIdx;
		endIdx = (
				endIdx + blockSize < NUM_PARTITIONS ?
						endIdx + blockSize : NUM_PARTITIONS);
	}
	for (i = 0; i < num_reducers; i++) {
		pthread_join(reduceThreads[i], NULL);
	}

	pthread_attr_destroy(&attr);
	pthread_mutex_destroy(&data_mutex);
}

int main(int argc, char *argv[]) {
	MR_Run(argc, argv, Map, 10, Reduce, 10, MR_DefaultHashPartition);
	return 0;
}
