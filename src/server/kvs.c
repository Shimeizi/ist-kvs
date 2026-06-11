#include "kvs.h"

#include <ctype.h>
#include <stdlib.h>

#include "string.h"
#include "constants.h"

// Hash function based on key initial.
// @param key Lowercase alphabetical string.
// @return hash.
// NOTE: This is not an ideal hash function, but is useful for test purposes of
// the project
int hash(const char *key) {
  int firstLetter = tolower(key[0]);
  if (firstLetter >= 'a' && firstLetter <= 'z') {
    return firstLetter - 'a';
  } else if (firstLetter >= '0' && firstLetter <= '9') {
    return firstLetter - '0';
  }
  return -1; // Invalid index for non-alphabetic or number strings
}

struct HashTable *create_hash_table() {
  HashTable *ht = malloc(sizeof(HashTable));
  if (!ht)
    return NULL;
  for (int i = 0; i < TABLE_SIZE; i++) {
    ht->table[i] = NULL;
  }
  pthread_rwlock_init(&ht->tablelock, NULL);
  return ht;
}

int write_pair(HashTable *ht, const char *key, const char *value) {
  int index = hash(key);

  // Search for the key node
  KeyNode *keyNode = ht->table[index];
  KeyNode *previousNode;

  while (keyNode != NULL) {
    if (strcmp(keyNode->key, key) == 0) {
      // overwrite value
      free(keyNode->value);
      keyNode->value = strdup(value);
      //Send the notification to the client about the updated key value
      ClientsNode *client = keyNode->clients;
      ClientsNode *prevClient = NULL;
      char key_str[MAX_STRING_SIZE + 1] = { '\0' };
      char val_str[MAX_STRING_SIZE + 1] = { '\0' };
      strcpy(key_str, keyNode->key);
      strcpy(val_str, keyNode->value);
      while(client != NULL) {
        write(client->client_fd, key_str, MAX_STRING_SIZE + 1);
        write(client->client_fd, val_str, MAX_STRING_SIZE + 1);
        prevClient = client;
        client = prevClient->next;
      }
      return 0;
    }
    previousNode = keyNode;
    keyNode = previousNode->next; // Move to the next node
  }
  // Key not found, create a new key node
  keyNode = malloc(sizeof(KeyNode));
  keyNode->key = strdup(key);       // Allocate memory for the key
  keyNode->value = strdup(value);   // Allocate memory for the value
  keyNode->clients = NULL;// point to a NULL pointer
  keyNode->next = ht->table[index]; // Link to existing nodes
  ht->table[index] = keyNode; // Place new key node at the start of the list
  return 0;
}

char *read_pair(HashTable *ht, const char *key) {
  int index = hash(key);

  KeyNode *keyNode = ht->table[index];
  KeyNode *previousNode;
  char *value;

  while (keyNode != NULL) {
    if (strcmp(keyNode->key, key) == 0) {
      value = strdup(keyNode->value);
      return value; // Return the value if found
    }
    previousNode = keyNode;
    keyNode = previousNode->next; // Move to the next node
  }

  return NULL; // Key not found
}

int delete_pair(HashTable *ht, const char *key) {
  int index = hash(key);

  // Search for the key node
  KeyNode *keyNode = ht->table[index];
  KeyNode *prevNode = NULL;

  while (keyNode != NULL) {
    if (strcmp(keyNode->key, key) == 0) {
      // Key found; delete this node
      if (prevNode == NULL) {
        // Node to delete is the first node in the list
        ht->table[index] =
            keyNode->next; // Update the table to point to the next node
      } else {
        // Node to delete is not the first; bypass it
        prevNode->next =
            keyNode->next; // Link the previous node to the next node
      }
      // Send notification to all clients about the deleted key and value
      ClientsNode *client = keyNode->clients;
      ClientsNode *prevClient = NULL;
      char key_str[MAX_STRING_SIZE + 1] = { '\0' };
      char del_str[MAX_STRING_SIZE + 1] = { '\0' };
      strcpy(key_str, keyNode->key);
      strcpy(del_str, "DELETED");
      while(client != NULL) {
        write(client->client_fd, key_str, MAX_STRING_SIZE + 1);
        write(client->client_fd, del_str, MAX_STRING_SIZE + 1);
        prevClient = client;
        client = prevClient->next;
        }
      // Free the memory allocated for the key and value
      free(keyNode->key);
      free(keyNode->value);
      free(keyNode); // Free the key node itself
      return 0;      // Exit the function
    }
    prevNode = keyNode;      // Move prevNode to current node
    keyNode = keyNode->next; // Move to the next node
  }
  return 1;
}

void free_table(HashTable *ht) {
  for (int i = 0; i < TABLE_SIZE; i++) {
    KeyNode *keyNode = ht->table[i];
    while (keyNode != NULL) {
      KeyNode *temp = keyNode;
      keyNode = keyNode->next;
      free(temp->key);
      free(temp->value);
      free(temp);
    }
  }
  pthread_rwlock_destroy(&ht->tablelock);
  free(ht);
}

KeyNode *key_exists(HashTable *ht, const char *key) {
  int index = hash(key);
  KeyNode *keyNode = ht->table[index];

  while (keyNode != NULL) {
    if (strcmp(keyNode->key, key) == 0) 
      return keyNode;
    keyNode = keyNode->next;
  }
  return NULL;
}

char key_subscribe(HashTable *ht, const char *key, int fd){
  KeyNode *keyNode;
  if((keyNode = key_exists(ht, key)) == NULL){
    return '0';
  }

  ClientsNode *clientNode = keyNode-> clients;
  ClientsNode *prev_client = NULL;
  while(clientNode != NULL){
    if (clientNode-> client_fd == fd) {          
      return '1';
    }
    prev_client = clientNode;
    clientNode = prev_client->next;
  }
  clientNode = malloc(sizeof(ClientsNode));
  clientNode->client_fd = fd;
  clientNode->next = keyNode-> clients;
  keyNode-> clients = clientNode; 
  
  return '0';
}

char key_unsubscribe(HashTable *ht, const char *key, int fd){
  KeyNode *keyNode;
  if((keyNode = key_exists(ht, key)) == NULL){
    return '1';
  }

  ClientsNode *clientNode = keyNode-> clients;
  ClientsNode *prev_client = NULL;
  while(clientNode != NULL){
    if (clientNode-> client_fd == fd) {
      if(prev_client == NULL){
        keyNode->clients = clientNode->next;
      } else{
        prev_client->next = clientNode->next;
      }
      free(clientNode);
      return '0';
    }
    prev_client = clientNode;
    clientNode = prev_client->next;
  }
  return '1';
}

void kill_all_clients(HashTable *ht){
  KeyNode *hash_list = NULL;
  ClientsNode *clientsNode = NULL;
  ClientsNode *prev_client = NULL;
  for (int i = 0; i < TABLE_SIZE; i++) {
    hash_list = ht->table[i];
    while(hash_list != NULL){
      clientsNode = hash_list->clients;
      while(clientsNode != NULL){
        prev_client = clientsNode;
        clientsNode = prev_client->next;   
        free(prev_client);
      }
      hash_list->clients = NULL;
      hash_list = hash_list->next;
    }
  }
}