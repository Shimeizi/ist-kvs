#include <dirent.h>
#include <fcntl.h>
#include <limits.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/types.h>
#include <sys/wait.h>
#include <unistd.h>
#include <errno.h>
#include <sys/stat.h>
#include <semaphore.h>
#include <signal.h>

#include "src/common/constants.h"
#include "src/common/io.h"
#include "constants.h"
#include "io.h"
#include "operations.h"
#include "parser.h"
#include "pthread.h"

#define PC_BUFFER_SIZE 3 * MAX_PIPE_PATH_LENGTH * MAX_SESSION_COUNT

int server_reg;
// Indicates if server has received a SIGUSR1 signal
int signal_received = 0;
//inputBuf
sem_t semEmpty;
sem_t semFull;
pthread_mutex_t mutexinput_buffer = PTHREAD_MUTEX_INITIALIZER;
char pc_buffer[PC_BUFFER_SIZE];
int p_count = 0;
int c_count = 0;

int fd_buffer[3 * MAX_SESSION_COUNT * sizeof(int)];
char paths_buffer[3 * MAX_SESSION_COUNT * MAX_PIPE_PATH_LENGTH];
int clients_num = 0;

void destroy_pathList();
void add_path(char *req_pipe_path, char *resp_pipe_path, char *notif_pipe_path, int client_req, int client_resp, int client_notif);

struct SharedData {
  DIR *dir;
  char *dir_name;
  pthread_mutex_t directory_mutex;
};

struct PipePathsFd {
  char* req_path;
  char* resp_path;
  char* notif_path;
  int req_fd;
  int resp_fd;
  int notif_fd;
  struct PipePathsFd *nextPipe;
};

struct PipePathsFd *pipepathsfd = NULL;
pthread_mutex_t mutexpipe_path = PTHREAD_MUTEX_INITIALIZER;

pthread_mutex_t lock = PTHREAD_MUTEX_INITIALIZER;
pthread_mutex_t n_current_backups_lock = PTHREAD_MUTEX_INITIALIZER;

size_t active_backups = 0; // Number of active backups
size_t max_backups;        // Maximum allowed simultaneous backups
size_t max_threads;        // Maximum allowed simultaneous threads
char *jobs_directory = NULL;

static void sig_handler() {
  // Received SIGUSR1 signal
  signal_received = 1;
}

static void sigpipe_handler() {
  // Received SIGPIPE signal
}

int filter_job_files(const struct dirent *entry) {
  const char *dot = strrchr(entry->d_name, '.');
  if (dot != NULL && strcmp(dot, ".job") == 0) {
    return 1; // Keep this file (it has the .job extension)
  }
  return 0;
}

static int entry_files(const char *dir, struct dirent *entry, char *in_path,
                       char *out_path) {
  const char *dot = strrchr(entry->d_name, '.');
  if (dot == NULL || dot == entry->d_name || strlen(dot) != 4 ||
      strcmp(dot, ".job")) {
    return 1;
  }

  if (strlen(entry->d_name) + strlen(dir) + 2 > MAX_JOB_FILE_NAME_SIZE) {
    fprintf(stderr, "%s/%s\n", dir, entry->d_name);
    return 1;
  }

  strcpy(in_path, dir);
  strcat(in_path, "/");
  strcat(in_path, entry->d_name);

  strcpy(out_path, in_path);
  strcpy(strrchr(out_path, '.'), ".out");

  return 0;
}

static int run_job(int in_fd, int out_fd, char *filename) {
  size_t file_backups = 0;
  while (1) {
    char keys[MAX_WRITE_SIZE][MAX_STRING_SIZE] = {0};
    char values[MAX_WRITE_SIZE][MAX_STRING_SIZE] = {0};
    unsigned int delay;
    size_t num_pairs;

    switch (get_next(in_fd)) {
    case CMD_WRITE:
      num_pairs =
          parse_write(in_fd, keys, values, MAX_WRITE_SIZE, MAX_STRING_SIZE);
      if (num_pairs == 0) {
        write_str(STDERR_FILENO, "Invalid command. See HELP for usage\n");
        continue;
      }

      if (kvs_write(num_pairs, keys, values)) {
        write_str(STDERR_FILENO, "Failed to write pair\n");
      }
      break;

    case CMD_READ:
      num_pairs =
          parse_read_delete(in_fd, keys, MAX_WRITE_SIZE, MAX_STRING_SIZE);

      if (num_pairs == 0) {
        write_str(STDERR_FILENO, "Invalid command. See HELP for usage\n");
        continue;
      }

      if (kvs_read(num_pairs, keys, out_fd)) {
        write_str(STDERR_FILENO, "Failed to read pair\n");
      }
      break;

    case CMD_DELETE:
      num_pairs =
          parse_read_delete(in_fd, keys, MAX_WRITE_SIZE, MAX_STRING_SIZE);

      if (num_pairs == 0) {
        write_str(STDERR_FILENO, "Invalid command. See HELP for usage\n");
        continue;
      }

      if (kvs_delete(num_pairs, keys, out_fd)) {
        write_str(STDERR_FILENO, "Failed to delete pair\n");
      }
      break;

    case CMD_SHOW:
      kvs_show(out_fd);
      break;

    case CMD_WAIT:
      if (parse_wait(in_fd, &delay, NULL) == -1) {
        write_str(STDERR_FILENO, "Invalid command. See HELP for usage\n");
        continue;
      }

      if (delay > 0) {
        printf("Waiting %d seconds\n", delay / 1000);
        kvs_wait(delay);
      }
      break;

    case CMD_BACKUP:
      pthread_mutex_lock(&n_current_backups_lock);
      if (active_backups >= max_backups) {
        wait(NULL);
      } else {
        active_backups++;
      }
      pthread_mutex_unlock(&n_current_backups_lock);
      int aux = kvs_backup(++file_backups, filename, jobs_directory);

      if (aux < 0) {
        write_str(STDERR_FILENO, "Failed to do backup\n");
      } else if (aux == 1) {
        return 1;
      }
      break;

    case CMD_INVALID:
      write_str(STDERR_FILENO, "Invalid command. See HELP for usage\n");
      break;

    case CMD_HELP:
      write_str(STDOUT_FILENO,
                "Available commands:\n"
                "  WRITE [(key,value)(key2,value2),...]\n"
                "  READ [key,key2,...]\n"
                "  DELETE [key,key2,...]\n"
                "  SHOW\n"
                "  WAIT <delay_ms>\n"
                "  BACKUP\n" // Not implemented
                "  HELP\n");

      break;

    case CMD_EMPTY:
      break;

    case EOC:
      printf("EOF\n");
      return 0;
    }
  }
}

// frees arguments
static void *get_file(void *arguments) {
  struct SharedData *thread_data = (struct SharedData *)arguments;
  DIR *dir = thread_data->dir;
  char *dir_name = thread_data->dir_name;

  if (pthread_mutex_lock(&thread_data->directory_mutex) != 0) {
    fprintf(stderr, "Thread failed to lock directory_mutex\n");
    return NULL;
  }

  struct dirent *entry;
  char in_path[MAX_JOB_FILE_NAME_SIZE], out_path[MAX_JOB_FILE_NAME_SIZE];
  while ((entry = readdir(dir)) != NULL) {
    if (entry_files(dir_name, entry, in_path, out_path)) {
      continue;
    }

    if (pthread_mutex_unlock(&thread_data->directory_mutex) != 0) {
      fprintf(stderr, "Thread failed to unlock directory_mutex\n");
      return NULL;
    }

    int in_fd = open(in_path, O_RDONLY);
    if (in_fd == -1) {
      write_str(STDERR_FILENO, "Failed to open input file: ");
      write_str(STDERR_FILENO, in_path);
      write_str(STDERR_FILENO, "\n");
      pthread_exit(NULL);
    }

    int out_fd = open(out_path, O_WRONLY | O_CREAT | O_TRUNC, 0666);
    if (out_fd == -1) {
      write_str(STDERR_FILENO, "Failed to open output file: ");
      write_str(STDERR_FILENO, out_path);
      write_str(STDERR_FILENO, "\n");
      pthread_exit(NULL);
    }

    int out = run_job(in_fd, out_fd, entry->d_name);

    close(in_fd);
    close(out_fd);

    if (out) {
      if (closedir(dir) == -1) {
        fprintf(stderr, "Failed to close directory\n");
        return 0;
      }

      exit(0);
    }

    if (pthread_mutex_lock(&thread_data->directory_mutex) != 0) {
      fprintf(stderr, "Thread failed to lock directory_mutex\n");
      return NULL;
    }
  }

  if (pthread_mutex_unlock(&thread_data->directory_mutex) != 0) {
    fprintf(stderr, "Thread failed to unlock directory_mutex\n");
    return NULL;
  }

  pthread_exit(NULL);
}

void *read_pipe(){
  struct sigaction sa;
  sa.sa_handler = sig_handler;
  sigemptyset(&sa.sa_mask);
  sa.sa_flags = 0;
  if (sigaction(SIGUSR1, &sa, NULL) != 0) {
    exit(EXIT_FAILURE);
  }

  memset(fd_buffer, 0, sizeof(fd_buffer));
  memset(paths_buffer, 0, sizeof(paths_buffer));

  sem_init(&semEmpty, 0, MAX_SESSION_COUNT);
  sem_init(&semFull, 0, 0);

  while(1){

    // Check if received SIGUSR1 signal
    if (signal_received) {
        pthread_mutex_lock(&mutexinput_buffer);

        struct PipePathsFd *pointer = pipepathsfd;
        while(pointer != NULL){
          close(pointer -> notif_fd);
          close(pointer -> req_fd);
          close(pointer -> resp_fd);

          unlink(pointer -> notif_path);
          unlink(pointer -> req_path);
          unlink(pointer -> resp_path);
        }
        destroy_pathList();
        kvs_killAllClients();
        signal_received = 0;
        pthread_mutex_unlock(&mutexinput_buffer);
        continue;
    }

    char op_code = 0;
    read(server_reg, &op_code, sizeof(char));

    if(op_code == '1'){
      sem_wait(&semEmpty);
      pthread_mutex_lock(&mutexinput_buffer);

      if(read(server_reg, pc_buffer + p_count, 3 * MAX_PIPE_PATH_LENGTH) < 0){
          perror("Error reading pc_buffer");
      }else{
        p_count = (p_count + 3 * MAX_PIPE_PATH_LENGTH) % PC_BUFFER_SIZE;
        sem_post(&semFull);
      }
      pthread_mutex_unlock(&mutexinput_buffer);
    }
  }
}

void add_path(char* req_path, char* resp_path, char* notif_path, int req_fd, int resp_fd, int notif_fd){
  struct PipePathsFd *paths = malloc(sizeof(struct PipePathsFd));
  paths->req_path = req_path;
  paths->resp_path = resp_path;
  paths->notif_path = notif_path;
  paths->req_fd = req_fd;
  paths->resp_fd = resp_fd;
  paths->notif_fd = notif_fd;
  paths->nextPipe = NULL;
  pthread_mutex_lock(&mutexpipe_path);
  if(pipepathsfd == NULL){
    pipepathsfd = paths;
  }
  else{
    paths->nextPipe = pipepathsfd;
    pipepathsfd-> nextPipe = paths;
  }
  pthread_mutex_unlock(&mutexpipe_path);
}

void disconnect_path(char* req_path){
  pthread_mutex_lock(&mutexpipe_path);
  struct PipePathsFd *pathNode = pipepathsfd;
  struct PipePathsFd *prevPath = NULL;
   while(pathNode != NULL){
    if (pathNode-> req_path == req_path) {
      if(prevPath == NULL){
        pipepathsfd = pathNode->nextPipe;
      } else{
        prevPath ->nextPipe = pathNode->nextPipe;
      }
      free(pathNode);
    }
    prevPath = pathNode;
    pathNode = prevPath->nextPipe;
  }
  pthread_mutex_unlock(&mutexpipe_path);
}

void destroy_pathList(){
  pthread_mutex_lock(&mutexpipe_path);
  struct PipePathsFd *pathNode = pipepathsfd;
  struct PipePathsFd *prevPath = NULL;
  while(pathNode != NULL){
    prevPath = pathNode;
    pathNode = prevPath->nextPipe;
    free(prevPath);
  }
  pipepathsfd = NULL;
  pthread_mutex_unlock(&mutexpipe_path);
}


void *client_thread(){

  // Blocks SIGUSR1 signals
  sigset_t mask;
  sigemptyset(&mask);
  sigaddset(&mask, SIGUSR1);
  if (pthread_sigmask(SIG_BLOCK, &mask, NULL) != 0) {
    exit(EXIT_FAILURE);
  }

  struct sigaction sa;
  sa.sa_handler = sigpipe_handler;
  sigemptyset(&sa.sa_mask);
  sa.sa_flags = 0;
  if (sigaction(SIGPIPE, &sa, NULL) != 0) {
    exit(EXIT_FAILURE);
  }

  while(1){
    char req_pipe_path[MAX_PIPE_PATH_LENGTH];
    char resp_pipe_path[MAX_PIPE_PATH_LENGTH];
    char notif_pipe_path[MAX_PIPE_PATH_LENGTH];

    memset(req_pipe_path, 0, MAX_PIPE_PATH_LENGTH);
    memset(resp_pipe_path, 0, MAX_PIPE_PATH_LENGTH);
    memset(notif_pipe_path, 0, MAX_PIPE_PATH_LENGTH);

    sem_wait(&semFull);
    pthread_mutex_lock(&mutexinput_buffer);
    memcpy(req_pipe_path, pc_buffer + c_count, MAX_PIPE_PATH_LENGTH);
    memcpy(resp_pipe_path, pc_buffer + c_count + MAX_PIPE_PATH_LENGTH, MAX_PIPE_PATH_LENGTH);
    memcpy(notif_pipe_path, pc_buffer + c_count + 2 * MAX_PIPE_PATH_LENGTH, MAX_PIPE_PATH_LENGTH);

    c_count = (c_count + 3 * MAX_PIPE_PATH_LENGTH) % PC_BUFFER_SIZE;

    pthread_mutex_unlock(&mutexinput_buffer);
    sem_post(&semEmpty);

    // Open req_pipe
    int client_req = open(req_pipe_path, O_RDONLY);
    if (client_req == -1) {
      fprintf(stderr, "[ERR]: Failed to open req_pipe: %s\n", strerror(errno));
    }
    pthread_mutex_lock(&lock);
    fd_buffer[clients_num] = client_req;
    clients_num++;
    pthread_mutex_unlock(&lock);

    // Open resp_pipe
    int client_resp = open(resp_pipe_path, O_WRONLY);
    if (client_resp == -1) {
      fprintf(stderr, "[ERR]: Failed to open resp_pipe: %s\n", strerror(errno));
      close(client_req);
    }
    pthread_mutex_lock(&lock);
    fd_buffer[clients_num] = client_resp;
    clients_num++;
    pthread_mutex_unlock(&lock);

    // Open notif_pipe
    int client_notif = open(notif_pipe_path, O_WRONLY);
    if (client_notif == -1) {
      fprintf(stderr, "[ERR]: Failed to open notif_pipe: %s\n", strerror(errno));
      close(client_req);
      close(client_resp);
    }
    pthread_mutex_lock(&lock);
    fd_buffer[clients_num] = client_notif;
    clients_num++;
    pthread_mutex_unlock(&lock);

    add_path(req_pipe_path, resp_pipe_path, notif_pipe_path, client_req, client_resp, client_notif);

    char keys[MAX_NUMBER_SUB][MAX_STRING_SIZE]; 
    char op_code = 0;
    int last_position = 0;
    while(1){
      if(read_all(client_req, &op_code, sizeof(char), NULL) < 0){
        break;
      }

      if(op_code == '2'){

        for (int i = 0; i < last_position; i++) {
        kvs_unsubscribe(keys[i], client_notif);
        memset(keys[i], 0, MAX_STRING_SIZE);
        }

        disconnect_path(req_pipe_path);

        // Close client_pipes
        close(client_req);
        close(client_resp);
        close(client_notif);

        unlink(req_pipe_path);
        unlink(resp_pipe_path);
        unlink(notif_pipe_path);
        break;
      }

      if(op_code == '3'){
        char key[MAX_STRING_SIZE];
        read(client_req, key, MAX_STRING_SIZE);
        char *result = malloc(sizeof(char));
        *result = kvs_subscribe(key, client_notif);

        if(strcmp(result, "0") == 0){
        strcpy(keys[last_position++], key);
        }

        write(client_resp, result, sizeof(char)); 
        free(result);
      }

      else if(op_code == '4'){
        char key[MAX_STRING_SIZE];
        read(client_req, key, MAX_STRING_SIZE);
        char *result = malloc(sizeof(char));
        *result = kvs_unsubscribe(key, client_notif);

        if(strcmp(result, "0") == 0){
          for (int i = 0; i < last_position; i++) {
            if (strcmp(keys[i], key) == 0) {
              for (int j = i; j < last_position - 1; j++) {
                  strcpy(keys[j], keys[j + 1]);
              }
              last_position--;
              i--;
            }
          }
        }

        write(client_resp, result, sizeof(char));
        free(result);
      }
    }
  }
  return NULL;
}

static void dispatch_threads(DIR *dir) {
  pthread_t *threads = malloc(max_threads * sizeof(pthread_t));
  pthread_t *threads_clients = malloc(MAX_NUMBER_SUB * sizeof(pthread_t));
  pthread_t read_pipe_thread; // thread para ler os 3 pipes do cliente

  if (threads == NULL) {
    fprintf(stderr, "Failed to allocate memory for threads\n");
    return;
  }

  struct SharedData thread_data = {dir, jobs_directory,
                                   PTHREAD_MUTEX_INITIALIZER};

  for (size_t i = 0; i < max_threads; i++) {
    if (pthread_create(&threads[i], NULL, get_file, (void *)&thread_data) != 0) {
      fprintf(stderr, "Failed to create thread %zu\n", i);
      pthread_mutex_destroy(&thread_data.directory_mutex);
      free(threads);
      return;
    }
  }

  if (pthread_create(&read_pipe_thread, NULL, read_pipe, NULL) != 0) {
    fprintf(stderr, "Failed to create thread %zu\n", max_threads);
    return;
  }

  if (threads_clients == NULL) {
    fprintf(stderr, "Failed to allocate memory for threads\n");
    return;
  }

  for(size_t j = 0; j < MAX_SESSION_COUNT; j++){
    if (pthread_create(&threads_clients[j], NULL, client_thread, NULL) != 0) {
      fprintf(stderr, "Failed to create thread %zu\n", j);
      return;
    }
  }

  for (unsigned int i = 0; i < max_threads; i++) {
    if (pthread_join(threads[i], NULL) != 0) {
      fprintf(stderr, "Failed to join thread %u\n", i);
      pthread_mutex_destroy(&thread_data.directory_mutex);
      free(threads);
      return;
    }
  }

  if (pthread_mutex_destroy(&thread_data.directory_mutex) != 0) {
    fprintf(stderr, "Failed to destroy directory_mutex\n");
  }

  free(threads);

  if (pthread_join(read_pipe_thread, NULL) != 0) {
    fprintf(stderr, "Failed to join read_pipe thread\n");
  }

  for (unsigned int i = 0; i < max_threads; i++) {
    if (pthread_join(threads_clients[i], NULL) != 0) {
      fprintf(stderr, "Failed to join thread %u\n", i);
      free(threads_clients);
      return;
    }
  }
}

int main(int argc, char **argv) {

  struct sigaction sa;
  sa.sa_handler = sig_handler;
  sigemptyset(&sa.sa_mask);
  sa.sa_flags = 0;
  if (sigaction(SIGUSR1, &sa, NULL) != 0) {
    exit(EXIT_FAILURE);
  }

  if (argc < 5) {
    write_str(STDERR_FILENO, "Usage: ");
    write_str(STDERR_FILENO, argv[0]);
    write_str(STDERR_FILENO, " <jobs_dir>");
    write_str(STDERR_FILENO, " <max_threads>");
    write_str(STDERR_FILENO, " <max_backups>");
    write_str(STDERR_FILENO, " <nome_do_FIFO_de_registo> \n");
    return 1;
  }

  jobs_directory = argv[1];

  char *endptr;
  max_backups = strtoul(argv[3], &endptr, 10);

  if (*endptr != '\0') {
    fprintf(stderr, "Invalid max_proc value\n");
    return 1;
  }

  max_threads = strtoul(argv[2], &endptr, 10);

  if (*endptr != '\0') {
    fprintf(stderr, "Invalid max_threads value\n");
    return 1;
  }

  if (max_backups <= 0) {
    write_str(STDERR_FILENO, "Invalid number of backups\n");
    return 0;
  }

  if (max_threads <= 0) {
    write_str(STDERR_FILENO, "Invalid number of threads\n");
    return 0;
  }

  if (kvs_init()) {
    write_str(STDERR_FILENO, "Failed to initialize KVS\n");
    return 1;
  }

  // Remove pipe if it does not exist
  if (unlink(argv[4]) != 0 && errno != ENOENT) {
    fprintf(stderr, "[ERR]: unlink(%s) failed: %s\n", argv[1],
            strerror(errno));
    exit(EXIT_FAILURE);
    return -1;
  }

  // Create pipe
  if (mkfifo(argv[4], 0666) != 0) {
    fprintf(stderr, "[ERR]: mkfifo failed: %s\n", strerror(errno));
    exit(EXIT_FAILURE);
    return 1;
  }
  
  // Open pipe for read and write
  server_reg = open(argv[4], O_RDONLY);
  if (server_reg == -1) {
    fprintf(stderr, "[ERR]: open failed: %s\n", strerror(errno));
    exit(EXIT_FAILURE);
    return 1;
  }

  DIR *dir = opendir(argv[1]);
  if (dir == NULL) {
    fprintf(stderr, "Failed to open directory: %s\n", argv[1]);
    return 0;
  }

  dispatch_threads(dir);

  if (closedir(dir) == -1) {
    fprintf(stderr, "Failed to close directory\n");
    return 0;
  }

  while (active_backups > 0) {
    wait(NULL);
    active_backups--;
  }

  kvs_terminate();

  return 0;
}
