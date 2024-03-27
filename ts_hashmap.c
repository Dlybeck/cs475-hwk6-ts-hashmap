#include <limits.h>
#include <pthread.h>
#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include "ts_hashmap.h"

pthread_mutex_t mutex;
/**
 * Creates a new thread-safe hashmap. 
 *
 * @param capacity initial capacity of the hashmap.
 * @return a pointer to a new thread-safe hashmap.
 */
ts_hashmap_t *initmap(int capacity) {
  ts_hashmap_t *hm = malloc(sizeof(ts_hashmap_t));
  hm->table = malloc(sizeof(ts_entry_t*) * capacity);
  hm->size = 0;
  hm->capacity = capacity;
  hm->numOps = 0;

  // Initialize the hashmap's table entries to NULL
  for (int i = 0; i < capacity; i++) {
    hm->table[i] = NULL;
  }

  //create lock
  pthread_mutex_init(&mutex, NULL);
  return hm;
}

/**
 * Obtains the value associated with the given key.
 * @param map a pointer to the map
 * @param key a key to search
 * @return the value associated with the given key, or INT_MAX if key not found
 */
int get(ts_hashmap_t *map, int key) {
  pthread_mutex_lock(&mutex);
  map->numOps++;
  int index = key % map->capacity;

  ts_entry_t *entry = map->table[index];

  //go through all entries looking for a match
  while (entry != NULL) {
    if (entry->key == key) {
      pthread_mutex_unlock(&mutex);
      return entry->value;
    }

    entry = entry->next;
  }

  //Key not found
  pthread_mutex_unlock(&mutex);
  return INT_MAX;
}

/**
 * Associates a value associated with a given key.
 * @param map a pointer to the map
 * @param key a key
 * @param value a value
 * @return old associated value, or INT_MAX if the key was new
 */
int put(ts_hashmap_t *map, int key, int value) {
  pthread_mutex_lock(&mutex);
  map->numOps++;
  unsigned int u_key = (unsigned int) key;

  //hash the key
  int index = u_key % map->capacity;

  ts_entry_t *entry = map->table[index];

  //if the chain is empty just add it
  if (entry == NULL) {
    ts_entry_t *new_entry = malloc(sizeof(ts_entry_t));
    new_entry->key = key;
    new_entry->value = value;
    new_entry->next = NULL;

    map->table[index] = new_entry;

    map->size++;
    pthread_mutex_unlock(&mutex);
    return INT_MAX;
  }

  //If the chain is not empty find the end
  while (entry->next != NULL) {
    if (entry->key == key) {
      int old_value = entry->value;
      entry->value = value;

      pthread_mutex_unlock(&mutex);
      return old_value;
    }
    entry = entry->next;
  }

  //Once entry->next is NULL add the new entry
  ts_entry_t *new_entry = malloc(sizeof(ts_entry_t));
  new_entry->key = key;
  new_entry->value = value;
  new_entry->next = NULL;

  entry->next = new_entry;

  map->size++;
  pthread_mutex_unlock(&mutex);
  return INT_MAX;
}

/**
 * Removes an entry in the map
 * @param map a pointer to the map
 * @param key a key to search
 * @return the value associated with the given key, or INT_MAX if key not found
 */
int del(ts_hashmap_t *map, int key) {
  pthread_mutex_lock(&mutex);
  map->numOps++;
  int index = key % map->capacity;

  ts_entry_t *entry = map->table[index];
  ts_entry_t *previous = NULL;

  //Look at every node
  while (entry != NULL) {
    //if this is the node to delete, remove it
    if (entry->key == key) {
      int value = entry->value;

      // Delete this key from hashmap
      if (previous != NULL) {
        previous->next = entry->next;
      } else {
        map->table[index] = entry->next;
      }

      // Free the memory for this entry
      free(entry);

      map->size--;
      pthread_mutex_unlock(&mutex);
      return value;
    }

    previous = entry;
    entry = entry->next;
  }

  pthread_mutex_unlock(&mutex);
  // Key not found
  return INT_MAX;
}


/**
 * Prints the contents of the map (given)
 */
void printmap(ts_hashmap_t *map) {
  for (int i = 0; i < map->capacity; i++) {
    printf("[%d] -> ", i);
    ts_entry_t *entry = map->table[i];
    while (entry != NULL) {
      printf("(%d,%d)", entry->key, entry->value);
      if (entry->next != NULL)
        printf(" -> ");
      entry = entry->next;
    }
    printf("\n");
  }
}

/**
 * Free up the space allocated for hashmap
 * @param map a pointer to the map
 */
void freeMap(ts_hashmap_t *map) {
  for (int i = 0; i < map->capacity; i++) {
    ts_entry_t *entry = map->table[i];
    while(entry != NULL){
      ts_entry_t *next_entry = entry->next;
      free(entry);
      entry = next_entry;
    }
  }
  free(map->table);
  free(map);

  pthread_mutex_destroy(&mutex);
}