#include <fcntl.h>
#include <pthread.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>

#include "parser.h"
#include "src/client/api.h"
#include "src/common/constants.h"
#include "src/common/io.h"

char req_pipe_path[256] = "/tmp/req";
char resp_pipe_path[256] = "/tmp/resp";
char notif_pipe_path[256] = "/tmp/notif";
char server_pipe_path[MAX_PIPE_PATH_LENGTH];

int* server_sig;

pthread_t cmd_thread, notif_thread;
pthread_mutex_t mutex = PTHREAD_MUTEX_INITIALIZER;

void *command_thread(){
  int disconnected = 1;
  char keys[MAX_NUMBER_SUB][MAX_STRING_SIZE] = {0};
  unsigned int delay_ms;
  size_t num;

  while (1) {

    pthread_mutex_lock(&mutex);
    if(*server_sig == 1){
      pthread_mutex_unlock(&mutex);
      break;
    }
    pthread_mutex_unlock(&mutex);

    switch (get_next(STDIN_FILENO)) {
      case CMD_DISCONNECT:
        if (kvs_disconnect() != 0) {
          fprintf(stderr, "Failed to disconnect to the server\n");
          return NULL;
          break;
        }
        unlink(req_pipe_path);
        unlink(resp_pipe_path);
        unlink(notif_pipe_path);
        unlink(server_pipe_path);

        pthread_cancel(cmd_thread);
        pthread_cancel(notif_thread);

        printf("Disconnected from server\n");
        disconnected = 0;
        break;

      case CMD_SUBSCRIBE:
        num = parse_list(STDIN_FILENO, keys, 1, MAX_STRING_SIZE);
        if (num == 0) {
          fprintf(stderr, "Invalid command. See HELP for usage\n");
          continue;
        }

        if (kvs_subscribe(keys[0])) {
          fprintf(stderr, "Command subscribe failed\n");
          return NULL;
        }

        break;

      case CMD_UNSUBSCRIBE:
        num = parse_list(STDIN_FILENO, keys, 1, MAX_STRING_SIZE);
        if (num == 0) {
          fprintf(stderr, "Invalid command. See HELP for usage\n");
          continue;
        }

        if (kvs_unsubscribe(keys[0])) {
          fprintf(stderr, "Command subscribe failed\n");
          return NULL;
        }

        break;

      case CMD_DELAY:
        if (parse_delay(STDIN_FILENO, &delay_ms) == -1) {
          fprintf(stderr, "Invalid command. See HELP for usage\n");
          continue;
        }

        if (delay_ms > 0) {
          printf("Waiting...\n");
          delay(delay_ms);
        }
        break;

      case CMD_INVALID:
        fprintf(stderr, "Invalid command. See HELP for usage\n");
        break;

      case CMD_EMPTY:
        break;

      case EOC:
        break;
      }
      if(disconnected == 0){
        break;
      }
  }
  return NULL;
}

void *get_notif(){
  while(1){
    
    kvs_notification(server_sig, &mutex);

    pthread_mutex_lock(&mutex);
    if(*server_sig == 1){
      pthread_mutex_unlock(&mutex);
      break;
    }
    pthread_mutex_unlock(&mutex);
  }
  return NULL;
}


int main(int argc, char *argv[]) {
  if (argc < 3) {
    fprintf(stderr, "Usage: %s <client_unique_id> <register_pipe_path>\n",
            argv[0]);
    return 1;
  }

  server_sig = malloc(sizeof(int));
  *server_sig = 2;

  strncat(req_pipe_path, argv[1], strlen(argv[1]) * sizeof(char));
  strncat(resp_pipe_path, argv[1], strlen(argv[1]) * sizeof(char));
  strncat(notif_pipe_path, argv[1], strlen(argv[1]) * sizeof(char));
  strncat(server_pipe_path, argv[2], strlen(argv[2]) * sizeof(char));

  if (kvs_connect(req_pipe_path, resp_pipe_path, argv[2], notif_pipe_path) != 0) {
    fprintf(stderr, "Failed to connect to the server\n");
    return 1;
  }

  if (pthread_create(&notif_thread, NULL, get_notif, NULL) != 0) {
    fprintf(stderr, "Failed to create thread\n");
  }

  if (pthread_create(&cmd_thread, NULL, command_thread, NULL) != 0) {
    fprintf(stderr, "Failed to create thread\n");
  }

 
  if (pthread_join(notif_thread, NULL) != 0) {
    fprintf(stderr, "Failed to join thread\n");
  }

  if (pthread_join(cmd_thread, NULL) != 0) {
    fprintf(stderr, "Failed to join thread\n");
  }

  free(server_sig);
  return 0;
}
