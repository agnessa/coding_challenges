#include <sys/types.h>
#include <sys/sysinfo.h>
#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <math.h>
#include "fnv_hash.h"
#include <errno.h>

extern int errno ;

typedef struct query_hash_with_offset {
   u_int64_t query;
   int offset;
} query_hash_with_offset_t;

/* qsort comparison function */ 
int query_hash_with_offset_cmp(const void * a, const void * b){ 
  const query_hash_with_offset_t * ia = (const query_hash_with_offset_t *)a;
  const query_hash_with_offset_t * ib = (const query_hash_with_offset_t *)b;
  return (ia->query > ib->query ? 1 : (ia->query < ib->query ? -1 : 0)); 
}

typedef struct query_rank {
   u_int64_t query;
   int offset;
   int cnt;
} query_rank_t;

/* qsort comparison function */ 
int query_rank_cmp(const void * a, const void * b){ 
  const query_rank_t * ia = (const query_rank_t *)a;
  const query_rank_t * ib = (const query_rank_t *)b;
  return (ia->cnt > ib->cnt ? -1 : (ia->cnt < ib->cnt ? 1 : 0)); 
}

#define MAX_CONCURRENT_RUNS 16

int get_avail_memory_size(){
  struct sysinfo info;
  sysinfo(&info);
  int total_mem = info.totalram / 8;
  int avail_mem = info.freeram;
  return (total_mem > avail_mem ? total_mem : avail_mem) / 2;
}

typedef struct merge_sort_conf {
   int data_run_cnt;
   int data_run_capacity;//no of records that fit into a single data run
   int input_buffer_cnt;
   int input_buffer_capacity;//no of records that fit into an input buffer
   int output_buffer_capacity;
   int batch_cnt;//no of batches of data runs to be merged at one time
   char input_filename[10];
   char output_filename[10];
} merge_sort_conf_t;

void merge_sort_conf_print(merge_sort_conf_t * conf){
  printf("data run capacity: %d\n", conf->data_run_capacity);
  printf("data run cnt: %d\n", conf->data_run_cnt);
  printf("input buffer cnt: %d\n", conf->input_buffer_cnt);
  printf("batch cnt: %d\n", conf->batch_cnt);
  printf("input buffer capacity: %d\n", conf->input_buffer_capacity);
  printf("output buffer capacity: %d\n", conf->output_buffer_capacity);
}

void merge_sort_conf_init(merge_sort_conf_t * conf){
  conf->data_run_capacity = get_avail_memory_size() / sizeof(query_hash_with_offset_t);
}

void merge_sort_conf_update(merge_sort_conf_t * conf, int data_run_cnt){
  conf->data_run_cnt = data_run_cnt;
  //calculate number of input buffers as minimum of data_run_cnt and MAX_CONCURRENT_RUNS
  conf->input_buffer_cnt = (conf->data_run_cnt < MAX_CONCURRENT_RUNS ? conf->data_run_cnt : MAX_CONCURRENT_RUNS);
  //calculate required number of algorithm passes
  conf->batch_cnt = ceil((float)conf->data_run_cnt / conf->input_buffer_cnt);
  //calculate input and output buffer size
  if(conf->input_buffer_cnt == 1){
    conf->output_buffer_capacity = conf->data_run_capacity;
    conf->input_buffer_capacity = conf->data_run_capacity;
  } else {
    conf->output_buffer_capacity = floor(conf->data_run_capacity / conf->input_buffer_cnt);
    conf->input_buffer_capacity = floor((conf->data_run_capacity - conf->output_buffer_capacity) / conf->input_buffer_cnt);
  }
}

typedef struct data_run {
  char filename[10];
  FILE * fd;
  int current_idx;
  int len;
  query_hash_with_offset_t * buffer;
} data_run_t;

typedef struct data_run_pool {
   data_run_t data_runs[MAX_CONCURRENT_RUNS];
   data_run_t output_data_run;
   int active_runs;
} data_run_pool_t;

char * data_run_filename(char * output_filename, int pass_idx, int data_run_idx){
  sprintf(output_filename, "run.%01d.%03d", pass_idx, data_run_idx);
  return output_filename;
}
/*
void data_run_print(data_run_t * dr, int len){
  int i;
  for (i=0; i<len; i++){
    printf("%llu, %d\n", dr->buffer[i].query, dr->buffer[i].offset);
  }
}
*/
void data_run_pool_init(merge_sort_conf_t * conf, data_run_pool_t * drp){
  int i;
  for(i=0; i<conf->input_buffer_cnt; i++){
    drp->data_runs[i].buffer = (query_hash_with_offset_t *)malloc(conf->input_buffer_capacity * sizeof(query_hash_with_offset_t));
  }
  drp->output_data_run.buffer = (query_hash_with_offset_t *)malloc(conf->output_buffer_capacity * sizeof(query_hash_with_offset_t));
}

void data_run_pool_free(merge_sort_conf_t * conf, data_run_pool_t * drp){
  int i;
  for(i=0; i<conf->input_buffer_cnt; i++){
    free(drp->data_runs[i].buffer);
  }
  free(drp->output_data_run.buffer);
}

void data_run_pool_open(merge_sort_conf_t * conf, data_run_pool_t * drp, int pass_idx, int batch_idx){
  int i;
  int first_data_run_idx = batch_idx * conf->input_buffer_cnt;
  for(i=0; i<drp->active_runs; i++){
    drp->data_runs[i].current_idx = 0;
    drp->data_runs[i].len = conf->input_buffer_capacity + 1;
    //open input buffer for reading
    data_run_filename(drp->data_runs[i].filename, pass_idx, first_data_run_idx + i);
    drp->data_runs[i].fd = fopen(drp->data_runs[i].filename, "r");
    if(drp->data_runs[i].fd == NULL)  {
      perror ("The following error occurred");
      exit(1);
    }
    //read data into buffer
    int items_read = fread(drp->data_runs[i].buffer, sizeof(query_hash_with_offset_t), conf->input_buffer_capacity, drp->data_runs[i].fd);
    if (items_read < conf->input_buffer_capacity)
      drp->data_runs[i].len = items_read;
  }     
  //open output buffer for writing
  drp->output_data_run.fd = fopen(data_run_filename(conf->output_filename, pass_idx + 1, batch_idx), "w+");
  if(drp->output_data_run.fd == NULL)  {
    perror ("The following error occurred");
    exit(1);
  }
  drp->output_data_run.current_idx = 0;
  drp->output_data_run.len = conf->input_buffer_capacity + 1;
}  

void data_run_pool_close(merge_sort_conf_t * conf, data_run_pool_t * drp){
  (void)conf;
  int i;
  for(i=0; i<drp->active_runs; i++){
    fclose(drp->data_runs[i].fd);
    //remove input file
    remove(drp->data_runs[i].filename);
  }
  
  fclose(drp->output_data_run.fd);
}

void data_run_pool_merge(merge_sort_conf_t * conf, data_run_pool_t * drp){
  int i;
  int min_idx, items_read;
  int data_to_merge = conf->input_buffer_cnt;
  query_hash_with_offset_t * tmp, * min_value;

  while(data_to_merge){
    //find initial minimum
    for(i=0; i<conf->input_buffer_cnt; i++){
      if (drp->data_runs[i].current_idx == -1)
        continue;
      min_idx = i;
      min_value = &drp->data_runs[min_idx].buffer[drp->data_runs[i].current_idx];
      break;
    }
    //find minimum
    for(i=min_idx; i<conf->input_buffer_cnt; i++){
      if(drp->data_runs[i].current_idx == -1)
        continue;
      tmp = &drp->data_runs[i].buffer[drp->data_runs[i].current_idx];
      if (tmp->query < min_value->query){
        min_idx = i;
        min_value = tmp;
      }
    }
    //save min in output buffer
    drp->output_data_run.buffer[drp->output_data_run.current_idx]=*min_value;
    //move current idx in output buffer
    drp->output_data_run.current_idx++;
    //flush to file if capacity exceeded
    if (drp->output_data_run.current_idx >= conf->output_buffer_capacity){
      fwrite(drp->output_data_run.buffer, sizeof(query_hash_with_offset_t), drp->output_data_run.current_idx, drp->output_data_run.fd);
      drp->output_data_run.current_idx = 0;
    }
    //move current idx in winning input buffer
    if (drp->data_runs[min_idx].current_idx < drp->data_runs[min_idx].len)
      drp->data_runs[min_idx].current_idx++;
    else {
      drp->data_runs[min_idx].current_idx = -1;
      data_to_merge--;
    }
    //read next bit if buffer empty
    if (drp->data_runs[min_idx].current_idx >= conf->input_buffer_capacity){
      items_read = fread(drp->data_runs[min_idx].buffer, sizeof(query_hash_with_offset_t), conf->input_buffer_capacity, drp->data_runs[min_idx].fd);
      if (items_read){
        drp->data_runs[min_idx].current_idx = 0;
      } else {
        drp->data_runs[min_idx].current_idx = -1;
        data_to_merge--;
      }
      drp->data_runs[min_idx].len = items_read;
    }
  }
  if(drp->output_data_run.current_idx){
    fwrite(drp->output_data_run.buffer, sizeof(query_hash_with_offset_t), drp->output_data_run.current_idx, drp->output_data_run.fd);
  }
}

void save_sorted_run(query_hash_with_offset_t * qhwo_run, int len, int idx){
  FILE * fp_out;
  char output_filename[10];
  //sort in memory
  qsort(qhwo_run, len, sizeof(query_hash_with_offset_t), query_hash_with_offset_cmp);
  //dump to file
  fp_out = fopen(data_run_filename(output_filename, 0, idx), "wb");
  fwrite(qhwo_run, sizeof(query_hash_with_offset_t), len, fp_out);
  fclose(fp_out);
}

void create_sorted_runs(merge_sort_conf_t * conf, char * input_filename){
  FILE * fp_in;
  char line[2048], * query;
  int offset = 0, i = 0, runs_cnt = 0, len;
  query_hash_with_offset_t qhwo;
  query_hash_with_offset_t * qhwo_run;
  //allocate memory for in memory sorting
  qhwo_run = (query_hash_with_offset_t *)malloc(conf->data_run_capacity * sizeof(query_hash_with_offset_t));
  fp_in = fopen(input_filename, "r");

  while(fgets(line,sizeof(line),fp_in) != NULL){
    len = strlen(line);
    //parse query from log line
    query = strchr((const char*) &line, ',') + 8;
    if (i == conf->data_run_capacity){
      save_sorted_run(qhwo_run, i, runs_cnt);
      runs_cnt++;
      i=0;
    }
    qhwo.query = fnv_64_buf(query, strlen(query)-2, FNV1_64_INIT);
    qhwo.offset = offset;
    qhwo_run[i++] = qhwo;
    offset += len;
  }
  if(i>0){
    //process remaining records
    save_sorted_run(qhwo_run, i, runs_cnt);
    runs_cnt++;
  }
  fclose(fp_in);

  free(qhwo_run);
  merge_sort_conf_update(conf, runs_cnt);
  //now there are runs_cnt in sorted runs in run.#n temp files
}

void merge_sorted_runs(merge_sort_conf_t * conf){
  data_run_pool_t drp;
  int i, pass_idx = 0;
  while(conf->data_run_cnt > 0){
    data_run_pool_init(conf, &drp);
    for(i = 0; i<conf->batch_cnt; i++){
      drp.active_runs = (conf->data_run_cnt - i * conf->input_buffer_cnt >= conf->input_buffer_cnt ? conf->input_buffer_cnt : conf->data_run_cnt % conf->input_buffer_cnt);
      data_run_pool_open(conf, &drp, pass_idx, i);
      data_run_pool_merge(conf, &drp);
      data_run_pool_close(conf, &drp);
    }  
    data_run_pool_free(conf, &drp);
    if (conf->data_run_cnt == 1)
      break;
    merge_sort_conf_update(conf, conf->batch_cnt);  
    pass_idx++;
  }
}

void topfive(merge_sort_conf_t * conf, char * input_filename){
  FILE * fd;
  query_rank_t topfive[5];
  query_hash_with_offset_t prev, * buffer;
  int items_read, cnt, i, j;
  char line[2048];

  buffer = (query_hash_with_offset_t *)malloc(conf->data_run_capacity * sizeof(query_hash_with_offset_t));
  for(i=0; i<5; i++){
    topfive[i].cnt = 0;
  }
  fd = fopen(conf->output_filename, "r");

  int total_items=0;
  while ((items_read = fread(buffer, sizeof(query_hash_with_offset_t), conf->data_run_capacity, fd))){
    int start_idx = 0;
    if (total_items == 0){
      prev = buffer[0];
      for(i=0;i<5;i++)
        topfive[i].cnt = 0;
      cnt = 1;
      start_idx = 1;
    }
    total_items += items_read;
    for (i=start_idx; i<items_read; i++){
      if (buffer[i].query != prev.query){
        //find min
        int min_idx = 0;
        int min_value = topfive[min_idx].cnt;
        min_idx = 0;
        min_value = topfive[min_idx].cnt;
        for (j=1; j<5; j++){
          if (topfive[j].cnt < min_value){
              min_idx = j;
              min_value = topfive[j].cnt;
          }
        }
	//replace min if winner found
        if (min_value < cnt){
          topfive[min_idx].cnt = cnt;
          topfive[min_idx].query = prev.query;
          topfive[min_idx].offset = prev.offset;
        }
        prev = buffer[i];
        cnt = 1;
      } else {
        cnt++;
      }
    }
  }

  fclose(fd);
  remove(conf->output_filename);

  //sort topfive
  qsort(&topfive, 5, sizeof(query_rank_t), query_rank_cmp);

  //for each in top five go to offset in file to print the query
  fd = fopen(input_filename, "r");
  for (i=0; i<5; i++){
    fseek(fd, topfive[i].offset, 0);
    fgets(line,sizeof(line),fd);
    line[strlen(line)-2] = '\0';
    printf("%s\n", strstr((const char*) &line, ",") + 8);
  }
  fclose(fd);
}

int main (int argc, char* argv[]){
  (void)argc;
  char * input_filename = argv[1];
  merge_sort_conf_t conf;
  merge_sort_conf_init(&conf);
  create_sorted_runs(&conf, input_filename);
  merge_sorted_runs(&conf);
  topfive(&conf, input_filename);
  return 0;
}
