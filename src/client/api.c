#include "api.h"
#include <errno.h>
#include <fcntl.h>
#include <stdbool.h>
#include <stdio.h>
#include <string.h>
#include <stdlib.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>

#include "src/common/constants.h"
#include "src/common/protocol.h"
#include "src/common/io.h"

int client_req;
int client_resp;
int client_notif;
int server_reg;
char result;

int kvs_connect(char const *req_pipe_path, char const *resp_pipe_path,
                char const *server_pipe_path, char const *notif_pipe_path) {
  // Remove pipe if it does not exist
  if (unlink(req_pipe_path) != 0 && errno != ENOENT) {
    fprintf(stderr, "[ERR]: unlink(%s) failed: %s\n", req_pipe_path,
            strerror(errno));
    exit(EXIT_FAILURE);
    return 1;
  }

  // Create pipe
  if (mkfifo(req_pipe_path, 0666) != 0) {
    fprintf(stderr, "[ERR]: mkfifo failed: %s\n", strerror(errno));
    exit(EXIT_FAILURE);
    return 1;
  }
  printf("Criou req_pipe\n");

  // Remove pipe if it does not exist
  if (unlink(resp_pipe_path) != 0 && errno != ENOENT) {
    fprintf(stderr, "[ERR]: unlink(%s) failed: %s\n", resp_pipe_path,
            strerror(errno));
    exit(EXIT_FAILURE);
    return 1;
  }

  // Create pipe
  if (mkfifo(resp_pipe_path, 0666) != 0) {
    fprintf(stderr, "[ERR]: mkfifo failed: %s\n", strerror(errno));
    exit(EXIT_FAILURE);
    return 1;
  }
  printf("Criou resp_pipe\n");

  // Remove pipe if it does not exist
  if (unlink(notif_pipe_path) != 0 && errno != ENOENT) {
    fprintf(stderr, "[ERR]: unlink(%s) failed: %s\n", notif_pipe_path,
            strerror(errno));
    exit(EXIT_FAILURE);
    return 1;
  }

  // Create pipe
  if (mkfifo(notif_pipe_path, 0666) != 0) {
    fprintf(stderr, "[ERR]: mkfifo failed: %s\n", strerror(errno));
    exit(EXIT_FAILURE);
    return 1;
  }
  printf("Criou notif_pipe\n");

  // Open pipe for writing
  server_reg = open(server_pipe_path, O_WRONLY);
  if (server_reg == -1) {
    fprintf(stderr, "[ERR]: open failed: %s\n", strerror(errno));
    exit(EXIT_FAILURE);
    return 1;
  }

  // Notify server to create pipes
  char buf[256];
  char op_code = '1';
  memset(buf, 0, 256);
  memcpy(buf, &op_code, sizeof(char));
  memcpy(buf + sizeof(char), req_pipe_path, 40);
  memcpy(buf + sizeof(char) + 40, resp_pipe_path, 40);
  memcpy(buf + sizeof(char) + 80, notif_pipe_path, 40);
  
  if (write(server_reg, buf, sizeof(char) + 120) == -1)
    return 1;

  // Open pipe for writing
  client_req = open(req_pipe_path, O_WRONLY);
  if (client_req == -1) {
    fprintf(stderr, "[ERR]: open failed: %s\n", strerror(errno));
    exit(EXIT_FAILURE);
    return 1;
  }
  printf("Abriu req_pipe\n");

  // Open pipe for reading
  client_resp = open(resp_pipe_path, O_RDONLY);
  if (client_resp == -1) {
    fprintf(stderr, "[ERR]: open failed: %s\n", strerror(errno));
    exit(EXIT_FAILURE);
    return 1;
  }
  printf("Abriu resp_pipe\n");

  // Open pipe for reading
  client_notif = open(notif_pipe_path, O_RDONLY);
  if (client_notif == -1) {
    fprintf(stderr, "[ERR]: open failed: %s\n", strerror(errno));
    exit(EXIT_FAILURE);
    return 1;
  }
  printf("Abriu notif_pipe\n");

  printf("Connect\n");

  return 0;
}

int kvs_disconnect(void) {
  // Notify server to close pipes
  char buf[256];
  char op_code = '2';
  memset(buf, 0, 256);
  memcpy(buf, &op_code, sizeof(char));

  if (write(client_req, buf, sizeof(char)) == -1){
    printf("Erro escrever no req_pipe\n");
    return 1;
  }

  // Close/unlike pipes
  close(client_req);
  close(client_resp);
  close(client_notif);
  close(server_reg);

  printf("Disconnect\n");
  
  return 0;
}

int kvs_subscribe(const char *key) {
  // send subscribe message to request pipe and wait for response in response
  // pipe

  // Subscribe key
  char buf[256];
  char op_code = '3';
  memset(buf, 0, 256);
  memcpy(buf, &op_code, sizeof(char));
  memcpy(buf + sizeof(char), key, 41);

  if (write(client_req, buf, sizeof(char) + 41) == -1) {
    perror("[ERR]: Write failed on client_req pipe");
    return 1;
  }
  

  read(client_resp, &result, sizeof(char)); 
  printf("%c\n", result);
  return 0;
}

int kvs_unsubscribe(const char *key) {
  // send unsubscribe message to request pipe and wait for response in response
  // pipe
  
  // Unsubscribe key
  char buf[256];
  char op_code = '4';
  memset(buf, 0, 256);
  memcpy(buf, &op_code, sizeof(char));
  memcpy(buf + sizeof(char), key, 41);

  if (write(client_req, buf, sizeof(char) + 41) == -1)
    return 1;

  read(client_resp, &result, sizeof(char)); 
  printf("%c\n", result);
  return 0;
}


void kvs_notification(int *intr, pthread_mutex_t *mutex_ptr) {
    char key[MAX_STRING_SIZE];
    char value[MAX_STRING_SIZE];
    
    if ((read_all(client_notif, key, MAX_STRING_SIZE, NULL) > 0) && 
        (read_all(client_notif, value, MAX_STRING_SIZE, NULL) > 0)) {

        if (strcmp(value, "DELETE") == 0) {
            printf("<%s><%s>\n", key, value);
        } else {
            printf("<%s>%s\n", key, value);
        }
    } else {
        pthread_mutex_lock(mutex_ptr);
        *intr = 1;
        pthread_mutex_unlock(mutex_ptr);
    }
}
