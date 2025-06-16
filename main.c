// Searching on a B+ Tree in C

#include <stdbool.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stdint.h>
#include <time.h>

// Default order
#define ORDER 5
#define MAX_KEY_LEN 100 

// CSV record structure
typedef struct {
    int id;
    char university[MAX_KEY_LEN];
    char department[MAX_KEY_LEN];
    float score;
} CSVRecord;

typedef struct {
    CSVRecord record; // The CSV record
    struct CSVRecordNode* next; // Pointer to the next node in the linked list   
} CSVRecordNode;

int csv_record_count = 0;
CSVRecord* records = NULL; // Dynamic array to hold CSV records


typedef struct record {
  CSVRecordNode* value;
} record;

// Node
typedef struct node {
  void **pointers;
  int *keys;
  struct node *parent;
  bool is_leaf;
  int num_keys;
  struct node *next;
} node;

int order = ORDER;
node *queue = NULL;
bool verbose_output = false;
int number_of_splits =0;

// Enqueue
void enqueue(node *new_node);

// Dequeue
node *dequeue(void);
int height(node *const root);
int pathToLeaves(node *const root, node *child);
void printLeaves(node *const root);
void printTree(node *const root);
void findAndPrint(node *const root, int key, bool verbose);
void findAndPrintRange(node *const root, int range1, int range2, bool verbose);
int findRange(node *const root, int key_start, int key_end, bool verbose,
        int returned_keys[], void *returned_pointers[]);
node *findLeaf(node *const root, int key, bool verbose);
record *find(node *root, int key, bool verbose, node **leaf_out);
int cut(int length);

record *makeRecord(CSVRecordNode* value);
node *makeNode(void);
node *makeLeaf(void);
int getLeftIndex(node *parent, node *left);
node *insertIntoLeaf(node *leaf, int key, record *pointer);
node *insertIntoLeafAfterSplitting(node *root, node *leaf, int key,
                   record *pointer);
node *insertIntoNode(node *root, node *parent,
           int left_index, int key, node *right);
node *insertIntoNodeAfterSplitting(node *root, node *parent,
                   int left_index,
                   int key, node *right);
node *insertIntoParent(node *root, node *left, int key, node *right);
node *insertIntoNewRoot(node *left, int key, node *right);
node *startNewTree(int key, record *pointer);
node *insert(node *root, int key, CSVRecordNode* value);

// Enqueue
void enqueue(node *new_node) {
  node *c;
  if (queue == NULL) {
    queue = new_node;
    queue->next = NULL;
  } else {
    c = queue;
    while (c->next != NULL) {
      c = c->next;
    }
    c->next = new_node;
    new_node->next = NULL;
  }
}

// Dequeue
node *dequeue(void) {
  node *n = queue;
  queue = queue->next;
  n->next = NULL;
  return n;
}

// Print the leaves
void printLeaves(node *const root) {
  if (root == NULL) {
    printf("Empty tree.\n");
    return;
  }
  int i;
  node *c = root;
  while (!c->is_leaf)
    c = c->pointers[0];
  while (true) {
    for (i = 0; i < c->num_keys; i++) {
      if (verbose_output)
        printf("%p ", c->pointers[i]);
      printf("%d ", c->keys[i]);
    }
    if (verbose_output)
      printf("%p ", c->pointers[order - 1]);
    if (c->pointers[order - 1] != NULL) {
      printf(" | ");
      c = c->pointers[order - 1];
    } else
      break;
  }
  printf("\n");
}

// Calculate height
int height(node *const root) {
  int h = 0;
  node *c = root;
  while (!c->is_leaf) {
    c = c->pointers[0];
    h++;
  }
  return h;
}

// Get path to root
int pathToLeaves(node *const root, node *child) {
  int length = 0;
  node *c = child;
  while (c != root) {
    c = c->parent;
    length++;
  }
  return length;
}

// Print the tree
void printTree(node *const root) {
  node *n = NULL;
  int i = 0;
  int rank = 0;
  int new_rank = 0;

  if (root == NULL) {
    printf("Empty tree.\n");
    return;
  }
  queue = NULL;
  enqueue(root);
  while (queue != NULL) {
    n = dequeue();
    if (n->parent != NULL && n == n->parent->pointers[0]) {
      new_rank = pathToLeaves(root, n);
      if (new_rank != rank) {
        rank = new_rank;
        printf("\n");
      }
    }
    if (verbose_output)
      printf("(%p)", n);
    for (i = 0; i < n->num_keys; i++) {
      if (verbose_output)
        printf("%p ", n->pointers[i]);
      printf("%d ", n->keys[i]);
    }
    if (!n->is_leaf)
      for (i = 0; i <= n->num_keys; i++)
        enqueue(n->pointers[i]);
    if (verbose_output) {
      if (n->is_leaf)
        printf("%p ", n->pointers[order - 1]);
      else
        printf("%p ", n->pointers[n->num_keys]);
    }
    printf("| ");
  }
  printf("\n");
}

// Find the node and print it
void findAndPrint(node *const root, int key, bool verbose) {
  node *leaf = NULL;
  record *r = find(root, key, verbose, NULL);
  if (r == NULL)
    printf("Record not found under key %d.\n", key);
  else
    printf("Record at %p -- key %d, value %d.\n",
         r, key, r->value);
}

// Find and print the range
void findAndPrintRange(node *const root, int key_start, int key_end,
             bool verbose) {
    int i;
    int array_size = key_end - key_start + 1;
    int *returned_keys = malloc(array_size * sizeof(int));
    void **returned_pointers = malloc(array_size * sizeof(void *));
    int num_found = findRange(root, key_start, key_end, verbose,
                returned_keys, returned_pointers);
    if (!num_found)
      printf("None found.\n");
    else {
      for (i = 0; i < num_found; i++)
        printf("Key: %d   Location: %p  Value: %d\n",
             returned_keys[i],
             returned_pointers[i],
             ((record *)
              returned_pointers[i])
               ->value);
  }
}

// Find the range
int findRange(node *const root, int key_start, int key_end, bool verbose,
        int returned_keys[], void *returned_pointers[]) {
  int i, num_found;
  num_found = 0;
  node *n = findLeaf(root, key_start, verbose);
  if (n == NULL)
    return 0;
  for (i = 0; i < n->num_keys && n->keys[i] < key_start; i++)
    ;
  if (i == n->num_keys)
    return 0;
  while (n != NULL) {
    for (; i < n->num_keys && n->keys[i] <= key_end; i++) {
      returned_keys[num_found] = n->keys[i];
      returned_pointers[num_found] = n->pointers[i];
      num_found++;
    }
    n = n->pointers[order - 1];
    i = 0;
  }
  return num_found;
}

// Find the leaf
node *findLeaf(node *const root, int key, bool verbose) {
  if (root == NULL) {
    if (verbose)
      printf("Empty tree.\n");
    return root;
  }
  int i = 0;
  node *c = root;
  while (!c->is_leaf) {
    if (verbose) {
      printf("[");
      for (i = 0; i < c->num_keys - 1; i++)
        printf("%d ", c->keys[i]);
      printf("%d] ", c->keys[i]);
    }
    i = 0;
    while (i < c->num_keys) {
      if (key >= c->keys[i])
        i++;
      else
        break;
    }
    if (verbose)
      printf("%d ->\n", i);
    c = (node *)c->pointers[i];
  }
  if (verbose) {
    printf("Leaf [");
    for (i = 0; i < c->num_keys - 1; i++)
      printf("%d ", c->keys[i]);
    printf("%d] ->\n", c->keys[i]);
  }
  return c;
}

record *find(node *root, int key, bool verbose, node **leaf_out) {
  if (root == NULL) {
    if (leaf_out != NULL) {
      *leaf_out = NULL;
    }
    return NULL;
  }

  int i = 0;
  node *leaf = NULL;

  leaf = findLeaf(root, key, verbose);

  for (i = 0; i < leaf->num_keys; i++)
    if (leaf->keys[i] == key)
      break;
  if (leaf_out != NULL) {
    *leaf_out = leaf;
  }
  if (i == leaf->num_keys)
    return NULL;
  else
    return (record *)leaf->pointers[i];
}

int cut(int length) {
  if (length % 2 == 0)
    return length / 2;
  else
    return length / 2 + 1;
}

record *makeRecord(CSVRecordNode* value) {
  record *new_record = (record *)malloc(sizeof(record));
  if (new_record == NULL) {
    perror("Record creation.");
    exit(EXIT_FAILURE);
  } else {
    new_record->value = value;
  }
  return new_record;
}

node *makeNode(void) {
  node *new_node;
  new_node = malloc(sizeof(node));
  if (new_node == NULL) {
    perror("Node creation.");
    exit(EXIT_FAILURE);
  }
  new_node->keys = malloc((order - 1) * sizeof(int));
  if (new_node->keys == NULL) {
    perror("New node keys array.");
    exit(EXIT_FAILURE);
  }
  new_node->pointers = malloc(order * sizeof(void *));
  if (new_node->pointers == NULL) {
    perror("New node pointers array.");
    exit(EXIT_FAILURE);
  }
  new_node->is_leaf = false;
  new_node->num_keys = 0;
  new_node->parent = NULL;
  new_node->next = NULL;
  return new_node;
}

node *makeLeaf(void) {
  node *leaf = makeNode();
  leaf->is_leaf = true;
  return leaf;
}

int getLeftIndex(node *parent, node *left) {
  int left_index = 0;
  while (left_index <= parent->num_keys &&
       parent->pointers[left_index] != left)
    left_index++;
  return left_index;
}

node *insertIntoLeaf(node *leaf, int key, record *pointer) {
  int i, insertion_point;

  insertion_point = 0;
  while (insertion_point < leaf->num_keys && leaf->keys[insertion_point] < key)
    insertion_point++;

  for (i = leaf->num_keys; i > insertion_point; i--) {
    leaf->keys[i] = leaf->keys[i - 1];
    leaf->pointers[i] = leaf->pointers[i - 1];
  }
  leaf->keys[insertion_point] = key;
  leaf->pointers[insertion_point] = pointer;
  leaf->num_keys++;
  return leaf;
}

node *insertIntoLeafAfterSplitting(node *root, node *leaf, int key, record *pointer) {
  number_of_splits++;
  node *new_leaf;
  int *temp_keys;
  void **temp_pointers;
  int insertion_index, split, new_key, i, j;

  new_leaf = makeLeaf();

  temp_keys = malloc(order * sizeof(int));
  if (temp_keys == NULL) {
    perror("Temporary keys array.");
    exit(EXIT_FAILURE);
  }

  temp_pointers = malloc(order * sizeof(void *));
  if (temp_pointers == NULL) {
    perror("Temporary pointers array.");
    exit(EXIT_FAILURE);
  }

  insertion_index = 0;
  while (insertion_index < order - 1 && leaf->keys[insertion_index] < key)
    insertion_index++;

  for (i = 0, j = 0; i < leaf->num_keys; i++, j++) {
    if (j == insertion_index)
      j++;
    temp_keys[j] = leaf->keys[i];
    temp_pointers[j] = leaf->pointers[i];
  }

  temp_keys[insertion_index] = key;
  temp_pointers[insertion_index] = pointer;

  leaf->num_keys = 0;

  split = cut(order - 1);

  for (i = 0; i < split; i++) {
    leaf->pointers[i] = temp_pointers[i];
    leaf->keys[i] = temp_keys[i];
    leaf->num_keys++;
  }

  for (i = split, j = 0; i < order; i++, j++) {
    new_leaf->pointers[j] = temp_pointers[i];
    new_leaf->keys[j] = temp_keys[i];
    new_leaf->num_keys++;
  }

  free(temp_pointers);
  free(temp_keys);

  new_leaf->pointers[order - 1] = leaf->pointers[order - 1];
  leaf->pointers[order - 1] = new_leaf;

  for (i = leaf->num_keys; i < order - 1; i++)
    leaf->pointers[i] = NULL;
  for (i = new_leaf->num_keys; i < order - 1; i++)
    new_leaf->pointers[i] = NULL;

  new_leaf->parent = leaf->parent;
  new_key = new_leaf->keys[0];

  return insertIntoParent(root, leaf, new_key, new_leaf);
}

node *insertIntoNode(node *root, node *n,
           int left_index, int key, node *right) {
  int i;

  for (i = n->num_keys; i > left_index; i--) {
    n->pointers[i + 1] = n->pointers[i];
    n->keys[i] = n->keys[i - 1];
  }
  n->pointers[left_index + 1] = right;
  n->keys[left_index] = key;
  n->num_keys++;
  return root;
}

node *insertIntoNodeAfterSplitting(node *root, node *old_node, int left_index,
                   int key, node *right) {
  number_of_splits++;
  int i, j, split, k_prime;
  node *new_node, *child;
  int *temp_keys;
  node **temp_pointers;

  temp_pointers = malloc((order + 1) * sizeof(node *));
  if (temp_pointers == NULL) {
    exit(EXIT_FAILURE);
  }
  temp_keys = malloc(order * sizeof(int));
  if (temp_keys == NULL) {
    exit(EXIT_FAILURE);
  }

  for (i = 0, j = 0; i < old_node->num_keys + 1; i++, j++) {
    if (j == left_index + 1)
      j++;
    temp_pointers[j] = old_node->pointers[i];
  }

  for (i = 0, j = 0; i < old_node->num_keys; i++, j++) {
    if (j == left_index)
      j++;
    temp_keys[j] = old_node->keys[i];
  }

  temp_pointers[left_index + 1] = right;
  temp_keys[left_index] = key;

  split = cut(order);
  new_node = makeNode();
  old_node->num_keys = 0;
  for (i = 0; i < split - 1; i++) {
    old_node->pointers[i] = temp_pointers[i];
    old_node->keys[i] = temp_keys[i];
    old_node->num_keys++;
  }
  old_node->pointers[i] = temp_pointers[i];
  k_prime = temp_keys[split - 1];
  for (++i, j = 0; i < order; i++, j++) {
    new_node->pointers[j] = temp_pointers[i];
    new_node->keys[j] = temp_keys[i];
    new_node->num_keys++;
  }
  new_node->pointers[j] = temp_pointers[i];
  free(temp_pointers);
  free(temp_keys);
  new_node->parent = old_node->parent;
  for (i = 0; i <= new_node->num_keys; i++) {
    child = new_node->pointers[i];
    child->parent = new_node;
  }

  return insertIntoParent(root, old_node, k_prime, new_node);
}

node *insertIntoParent(node *root, node *left, int key, node *right) {
  int left_index;
  node *parent;

  parent = left->parent;

  if (parent == NULL)
    return insertIntoNewRoot(left, key, right);

  left_index = getLeftIndex(parent, left);

  if (parent->num_keys < order - 1)
    return insertIntoNode(root, parent, left_index, key, right);

  return insertIntoNodeAfterSplitting(root, parent, left_index, key, right);
}

node *insertIntoNewRoot(node *left, int key, node *right) {
  node *root = makeNode();
  root->keys[0] = key;
  root->pointers[0] = left;
  root->pointers[1] = right;
  root->num_keys++;
  root->parent = NULL;
  left->parent = root;
  right->parent = root;
  return root;
}

node *startNewTree(int key, record *pointer) {
  node *root = makeLeaf();
  root->keys[0] = key;
  root->pointers[0] = pointer;
  root->pointers[order - 1] = NULL;
  root->parent = NULL;
  root->num_keys++;
  return root;
}

node *insert(node *root, int key, CSVRecordNode* value) {
  record *record_pointer = NULL;
  node *leaf = NULL;

  record_pointer = find(root, key, false, NULL);
  if (record_pointer != NULL) {
    record_pointer->value = value;
    return root;
  }

  record_pointer = makeRecord(value);

  if (root == NULL)
    return startNewTree(key, record_pointer);

  leaf = findLeaf(root, key, false);

  if (leaf->num_keys < order - 1) {
    leaf = insertIntoLeaf(leaf, key, record_pointer);
    return root;
  }

  return insertIntoLeafAfterSplitting(root, leaf, key, record_pointer);
}


void read_file(){
    FILE *file = fopen("yok_atlas.csv", "r");
    if (file == NULL) {
        perror("Error opening file");
        return;
    }

    char buffer[512];
    int line_count = 0;
    while (fgets(buffer, sizeof(buffer), file) != NULL) {
        if(line_count != 0){
            // skip header and process data

            CSVRecord record; // initialize record
            char *line = buffer;

            char *token = strsep(&line, ",");
            if (token != NULL) {
                record.id = atoi(token);
            }

            token = strsep(&line, ",");
            if (token != NULL) {
                strncpy(record.university, token, MAX_KEY_LEN);
            }

            token = strsep(&line, ",");
            if (token != NULL) {
                strncpy(record.department, token, MAX_KEY_LEN);
            }

            token = strsep(&line, ",");
            if (token != NULL) {
                record.score = atof(token);
            }

            // add record to records array
            records = (CSVRecord *)realloc(records, sizeof(CSVRecord) * (csv_record_count + 1));
            if (records == NULL) {
                perror("Error reallocating memory");
                fclose(file);
                return;
            }

            // Ensure strings are null-terminated
            record.university[MAX_KEY_LEN - 1] = '\0';
            record.department[MAX_KEY_LEN - 1] = '\0';

            records[csv_record_count] = record;
            csv_record_count++;
        }

        line_count++;
    }

    fclose(file);
}


uint32_t DJB2_hash(const uint8_t *str)
{
    uint32_t hash = 5381;
    uint8_t c;
    while ((c = *str++))
        hash = ((hash << 5) + hash) + c; /* hash * 33 + c */
    return hash;
}


// B + tree with bulk load 

typedef struct Node_bulkload {
    bool is_leaf;
    int num_keys;
    int keys[ORDER - 1];
    struct Node* children[ORDER];   // Internal: children; Leaf: unused
    struct Node* next;             // Leaf node linked list
    void* values[ORDER - 1];       // Leaf only

     CSVRecordNode* linked_list_node; // Leaf only, used to store CSV records
} Node_bulkload;


// Create a new node
Node_bulkload* create_node_bulkload(bool is_leaf) {
    Node_bulkload* node = (Node_bulkload*)malloc(sizeof(Node_bulkload));
    node->is_leaf = is_leaf;
    node->num_keys = 0;
    node->next = NULL;
    node->linked_list_node = NULL; // Initialize linked list node to NULL
    for (int i = 0; i < ORDER; i++) {
        node->children[i] = NULL;
    }
    for (int i = 0; i < ORDER - 1; i++) {
        node->values[i] = NULL;
    }
    return node;
}

// Bulk load sorted keys into leaf nodes
Node_bulkload* bulk_load(int* keys, int count) {
    int i = 0;
    Node_bulkload* head = NULL;
    Node_bulkload* prev = NULL;

    while (i < count) {
        Node_bulkload* leaf = create_node_bulkload(true);
        for (int j = 0; j < ORDER - 1 && i < count; j++, i++) {
            leaf->keys[j] = keys[i];
            leaf->values[j] = NULL;  // Initialize to NULL
            leaf->num_keys++;
        }

        if (prev) {
            prev->next = leaf;
        } else {
            head = leaf;
        }

        prev = leaf;
    }

    // No internal tree building for simplicity
    return head;  // Return first leaf node
}

// Search key in leaf nodes (simple linear scan)
Node_bulkload* search_bulkload(Node_bulkload* leaf_head, int key) {
    Node_bulkload* curr = leaf_head;
    while (curr) {
        for (int i = 0; i < curr->num_keys; i++) {
            if (curr->keys[i] == key) {
                return curr;  // Return the node containing the key
            }
        }
        curr = curr->next;
    }
    return NULL;  // Not found
}

// Comparison function for qsort
int compare_uint32(const void* a, const void* b) {
    uint32_t arg1 = *(const uint32_t*)a;
    uint32_t arg2 = *(const uint32_t*)b;
    return (arg1 > arg2) - (arg1 < arg2);
}

// Sorts and removes duplicates in-place
void sort_and_deduplicate(uint32_t* keys, int* count) {
    if (*count <= 1) return;

    // Step 1: Sort
    qsort(keys, *count, sizeof(uint32_t), compare_uint32);

    // Step 2: Deduplicate
    int unique_idx = 0;
    for (int i = 1; i < *count; i++) {
        if (keys[i] != keys[unique_idx]) {
            unique_idx++;
            keys[unique_idx] = keys[i];
        }
    }

    // Update count to number of unique elements
    *count = unique_idx + 1;
}


// util functions

int calculateHeight(node *root){
  if (root == NULL) {
      return 0;
  }
  int max_height = 0;
  for(int i =0 ; i< root->num_keys; i++){
    if(root->pointers[i] == NULL){
      continue;
    }
    int cur_height = calculateHeight(root->pointers[i]) + 1;
    max_height = max_height > cur_height ? max_height : cur_height;

  }
  return max_height;
}

unsigned long long estimateMemoryUsage(node *root){
    if (root == NULL) {
      return 0;
    }
  unsigned long long memory_usage = sizeof(root->pointers) + sizeof(root->keys) + sizeof(root->parent) + sizeof(root->is_leaf) + sizeof(root->num_keys) + sizeof(root->next);
  for(int i =0 ; i< root->num_keys; i++){
    if(root->pointers[i] == NULL){
      continue;
    }
    if(root->is_leaf){
        
        // calculate linked list memory usage
        record *r = (record *)root->pointers[i];
        CSVRecordNode *linked_list_node = (CSVRecordNode *)r->value;

        while (linked_list_node) {
            memory_usage += sizeof(CSVRecordNode);
            linked_list_node = linked_list_node->next;
        }
      }


    memory_usage += estimateMemoryUsage(root->pointers[i]) + 1;

  }
  return memory_usage;
}


int main(int argc, char* argv[]) {
    if (argc < 2) {
        printf("Usage: %s <number>\n", argv[0]);
        return 1;
    }
    read_file();
    bool is_bulk_load = atoi(argv[1]) == 2;

    if(!is_bulk_load){
          node *root;
          root = NULL;

         // sequentially insert records into the B+ tree
         for (int i = 0; i < csv_record_count; i++) {
             uint32_t key = DJB2_hash((const uint8_t *)records[i].department);
             root = insert(root, key, NULL);
         }
       
         // populate linked list 
         for (int i = 0; i < csv_record_count; i++) {
             uint32_t key = DJB2_hash((const uint8_t *)records[i].department);
             record *found = find(root, key, false, NULL);
             CSVRecordNode *linked_list_node = found->value;
        
        
             CSVRecordNode *new_node = (CSVRecordNode*)malloc(sizeof(CSVRecordNode));
             strcpy(new_node->record.department, records[i].department);
             strcpy(new_node->record.university, records[i].university);
             new_node->record.id = records[i].id;
             new_node->record.score = records[i].score;
             new_node->next = NULL;
        
             if(linked_list_node == NULL){
                 found->value = new_node;
             }
             else{
                 // go to last node in linked list
                 while (linked_list_node->next != NULL) {
                     linked_list_node = linked_list_node->next;
                 }
                 linked_list_node->next = new_node;
             }
        }
      

        // diplay split count
        printf("Number of splits: %d\n", number_of_splits);
        // display height of the tree
        int height_of_tree = calculateHeight(root);
        printf("Height of the tree: %d\n", height_of_tree);
        // display memory usage
        unsigned long long memory_usage = estimateMemoryUsage(root);
        printf("Estimated memory usage: %llu bytes\n", memory_usage);

          bool flag = true;
        while (flag)
        {
            char departmentNameInput[100];
            char rankInput[100];
            printf("Please enter the department name to search: \n");
            fgets(departmentNameInput, sizeof(departmentNameInput), stdin);
            departmentNameInput[strcspn(departmentNameInput, "\n")] = '\0';
        
            printf("Please enter the rank to search: \n");
            fgets(rankInput, sizeof(rankInput), stdin);
            rankInput[strcspn(rankInput, "\n")] = '\0';
        
            clock_t start_time = clock();
            record* result = find(root, DJB2_hash((const uint8_t *)departmentNameInput), false, NULL);
            clock_t end_time = clock();
            double seek_time_ms = 1e3 * (double)(end_time - start_time) / CLOCKS_PER_SEC;

            if (result == NULL) {
                printf("No records found for department: %s\n", departmentNameInput);
                continue;
            }

            CSVRecordNode* linked_list_node = result->value;
        
            if (linked_list_node == NULL) {
                printf("No records found for department: %s\n", departmentNameInput);
            } else {
                for(int i = 1 ; i < atoi(rankInput) && linked_list_node != NULL; i++) {
                    linked_list_node = linked_list_node->next;
                }
                printf("ID: %d, University: %s, Department: %s, Score: %.2f\n",
                       linked_list_node->record.id,
                       linked_list_node->record.university,
                       linked_list_node->record.department,
                       linked_list_node->record.score);
                      
                printf("Seek time: %.0f ms\n", seek_time_ms);
            }
          
        }
      
    }
    else{
      

      uint32_t* keys = malloc(csv_record_count * sizeof(uint32_t));
      for (int i = 0; i < csv_record_count; i++) {
          uint32_t key = DJB2_hash((const uint8_t *)records[i].department);
          keys[i] = key;
      }

      sort_and_deduplicate(keys, &csv_record_count);

      Node_bulkload* root = bulk_load(keys, csv_record_count);

     for (int i = 0; i < csv_record_count; i++) {
             uint32_t key = DJB2_hash((const uint8_t *)records[i].department);
             Node_bulkload *found = search_bulkload(root, key);
        
                // populate linked list 
  
             CSVRecordNode *linked_list_node = found->linked_list_node;
        
        
             CSVRecordNode *new_node = (CSVRecordNode*)malloc(sizeof(CSVRecordNode));
             strcpy(new_node->record.department, records[i].department);
             strcpy(new_node->record.university, records[i].university);
             new_node->record.id = records[i].id;
             new_node->record.score = records[i].score;
             new_node->next = NULL;
        
             if(linked_list_node == NULL){
                 found->linked_list_node = new_node;
             }
             else{
                 // go to last node in linked list
                 while (linked_list_node->next != NULL) {
                     linked_list_node = linked_list_node->next;
                 }
                 linked_list_node->next = new_node;
             }
        }
      

        bool flag = true;
        while (flag)
        {
            char departmentNameInput[100];
            char rankInput[100];
            printf("Please enter the department name to search: \n");
            fgets(departmentNameInput, sizeof(departmentNameInput), stdin);
            departmentNameInput[strcspn(departmentNameInput, "\n")] = '\0';
        
            printf("Please enter the rank to search: \n");
            fgets(rankInput, sizeof(rankInput), stdin);
            rankInput[strcspn(rankInput, "\n")] = '\0';
        
            Node_bulkload* result = search_bulkload(root, DJB2_hash((const uint8_t *)departmentNameInput));
        
            if (result == NULL) {
                printf("No records found for department: %s\n", departmentNameInput);
                continue;
            }

            CSVRecordNode* linked_list_node = result->linked_list_node;
        
            int counter = 0;
            if (linked_list_node == NULL) {
                printf("No records found for department: %s\n", departmentNameInput);
            } else {
                for(int i = 1 ; i < atoi(rankInput) && linked_list_node != NULL; i++) {
                  if(counter >= 1000) break;
                  if(linked_list_node->record.department != NULL && strcmp(linked_list_node->record.department, departmentNameInput) != 0){
                    i--;
                    counter++;
                    continue;
                  }
                  linked_list_node = linked_list_node->next;
                }

                if(counter >= 1000){
                  printf("No records found for department: %s\n", departmentNameInput);
                  continue;
                }
                else if(strcmp(linked_list_node->record.department, departmentNameInput) != 0){
                  printf("No records found for department: %s\n", departmentNameInput);
                  continue;
                }
                else{
                  
                  printf("ID: %d, University: %s, Department: %s, Score: %.2f\n",
                         linked_list_node->record.id,
                         linked_list_node->record.university,
                         linked_list_node->record.department,
                         linked_list_node->record.score);
                }


            }
          
        }
      
        
      }
      
    
    return 0;
}