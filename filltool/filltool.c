#include<stdio.h>
#include<string.h>
#include<stdlib.h>
void load_data(char* filename);
void label_data();

typedef struct disjoint_set_element {
   int rank;
   struct disjoint_set_element * parent;
} disjoint_set_element_t;

disjoint_set_element_t* ds_find(disjoint_set_element_t* e){
  while(e->parent != NULL)
    e = e->parent;
  return e;
}

disjoint_set_element_t* ds_union(disjoint_set_element_t* e1, disjoint_set_element_t* e2){
  disjoint_set_element_t* r1 = ds_find(e1);
  disjoint_set_element_t* r2 = ds_find(e2);
  disjoint_set_element_t* res;
  if(r1 == r2)
    return r1;
  if(r1->rank > r2->rank){
    r2->parent = r1;
    res = r1;
  } else if(r1->rank < r2->rank){
    r1->parent = r2;
    res = r2;
  } else {
    r2->parent = r1;
    r1->rank++;
    res = r1;
  }
  return res;
}

int** data;
disjoint_set_element_t** equi_labels;
int rows, cols;

int main (int argc, char* argv[]){
  (void)argc;
  load_data(argv[1]);
  label_data();
  return 0;
}

void label_data(){
  equi_labels = (disjoint_set_element_t**)calloc((cols*rows)/4, sizeof(disjoint_set_element_t*));
  disjoint_set_element_t* ds_ele;
  int label_cnt = 1;
  int i, j, k, ele, tmp, final_cnt;
  int neighbours[4];
  int min_neighbour, neighbour_cnt;
  for(i = 0; i<rows; i++){
    for(j=0; j<cols; j++){
      ele = data[i][j];
      if(ele == 0)
        continue;
      min_neighbour = label_cnt;
      neighbour_cnt = 0;
      if(i>0){
        //ne
        if(j<(cols-1) && (tmp = data[i-1][j+1]) > 0)
          neighbours[neighbour_cnt++] = tmp;
        //n
        if((tmp = data[i-1][j]) > 0)
          neighbours[neighbour_cnt++] = tmp;
        //nw
        if(j>0 && (tmp = data[i-1][j-1]) > 0)
          neighbours[neighbour_cnt++] = tmp;
      }
      //w
      if(j>0 && (tmp = data[i][j-1]) > 0)
        neighbours[neighbour_cnt++] = tmp;
     
      for(k=0; k<neighbour_cnt; k++){
	  if(neighbours[k] < min_neighbour)
	    min_neighbour = neighbours[k];
      }

      if(neighbour_cnt == 0){
        //new disjoint set for label_cnt
        ds_ele = (disjoint_set_element_t*)calloc(1,sizeof(disjoint_set_element_t));
        equi_labels[label_cnt-1]=ds_ele;
        data[i][j] = label_cnt++;
        continue;    
      }
      //label with min neighbour
      data[i][j] = min_neighbour;
      //for each neighbour label add min neighbour to equi labels
      for(k=0; k<neighbour_cnt && neighbours[k] != 0; k++){
        equi_labels[neighbours[k]-1] = ds_union(equi_labels[neighbours[k]-1],equi_labels[min_neighbour-1]);
        neighbours[k] = 0;
      }
    }
  }

  final_cnt = --label_cnt;
  for(i=0; i<label_cnt; i++){
    equi_labels[i] = ds_find(equi_labels[i]);
    for(j=0; j<i; j++){
      if(equi_labels[i] == equi_labels[j]){
        final_cnt--;
        equi_labels[j] = NULL;
      }
    }
  }
  printf("%d\n",final_cnt);
}

void load_data(char* filename){
  FILE* fp;
  char str[200];
  int line_no = -1;

  fp = fopen (filename, "r");
  while(fgets(str,sizeof(str),fp) != NULL){
    // strip trailing '\n'
    int len = strlen(str)-1;
    int i;
    if(str[len] == '\n')
      str[len] = 0;
    if (line_no < 0){
      sscanf(str,"%d %d",&rows, &cols);
      data = (int**)calloc(rows, sizeof(int*));
    } else {
      data[line_no] = (int*)calloc(cols, sizeof(int));
      for(i=0; i<cols; i++){
        if (str[i] == '0'){
          data[line_no][i] = 1;
        } else {
          data[line_no][i] = 0;
        }
      }
      
    }
    line_no++;
  }
  fclose(fp);
}
