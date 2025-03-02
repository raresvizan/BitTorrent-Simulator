#include <mpi.h>
#include <pthread.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#define TRACKER_RANK 0
#define MAX_FILES 10
#define MAX_FILENAME 15
#define HASH_SIZE 32
#define MAX_CHUNKS 100
#define TAG_INFO 0
#define TAG_SWARM 1
#define TAG_CHUNK 2
#define TAG_UPDATE 3
#define TAG_UPLOAD 4
#define TAG_FINISH 5

typedef struct {
	int client;
	int chunk_id[MAX_CHUNKS];
} Swarm;

typedef struct {
	int own;
	char hash[HASH_SIZE];
} Chunk;

typedef struct {
	char file_name[MAX_FILENAME];
	int total_chunks;
	int owned_chunks;
	Chunk chunks[MAX_CHUNKS];
	Swarm swarm[MAX_CHUNKS];
	int swarm_id;
} FileInfo;

FileInfo files[MAX_FILES];
int nr_files, nr_req_files;

int find_client(int file_id, int chunk_id)
{
	int possible_clients[files[file_id].swarm_id];
	int client_load[files[file_id].swarm_id];
	int cnt = 0;

	// clients that have the requested chunk
	for (int i = 0; i < files[file_id].swarm_id; i++) {
		if (files[file_id].swarm[i].chunk_id[chunk_id] == 1) {
			possible_clients[cnt] = files[file_id].swarm[i].client;
			client_load[cnt] = 0;

			// calculate load for each client
			for (int j = 0; j < files[file_id].total_chunks; j++) {
				if (files[file_id].swarm[i].chunk_id[j] == 1) {
					client_load[cnt]++;
				}
			}
			cnt++;
		}
	}

	// choose a free client
	if (cnt > 0) {
		int min_load_index = 0;
		for (int i = 1; i < cnt; i++) {
			if (client_load[i] < client_load[min_load_index]) {
				min_load_index = i;
			}
		}
		return possible_clients[min_load_index];
	}

	return -1;
}

int find_unowned_chunk(int file_id)
{
	for (int i = 0; i < files[file_id].total_chunks; i++) {
		if (files[file_id].chunks[i].own == 0) {
			return i;
		}
	}
	return -1;
}

void req_recv_swarm(int rank)
{
	MPI_Status status;

	for (int i = nr_files; i < nr_files + nr_req_files; i++) {
		MPI_Send(files[i].file_name, MAX_FILENAME, MPI_CHAR, TRACKER_RANK, TAG_SWARM, MPI_COMM_WORLD);
		// printf("[Peer %d] Requested swarm for file %s\n", rank, files[i].file_name);
		MPI_Recv(&files[i].swarm_id, 1, MPI_INT, TRACKER_RANK, TAG_SWARM, MPI_COMM_WORLD, &status);
		MPI_Recv(&files[i].total_chunks, 1, MPI_INT, TRACKER_RANK, TAG_SWARM, MPI_COMM_WORLD, &status);

		files[i].owned_chunks = 0;
		for (int j = 0; j < files[i].total_chunks; j++) {
			files[i].chunks[j].own = 0;
		}

		for (int j = 0; j < files[i].swarm_id; j++) {
			MPI_Recv(&files[i].swarm[j].client, 1, MPI_INT, TRACKER_RANK, TAG_SWARM, MPI_COMM_WORLD, &status);
			MPI_Recv(files[i].swarm[j].chunk_id, files[i].total_chunks, MPI_INT, TRACKER_RANK, TAG_SWARM,
					 MPI_COMM_WORLD, &status);
		}
		// printf("[Peer %d] Received swarm for file %s\n", rank, files[i].file_name);
	}
}

void write_file(int rank, int i)
{
	char out_file[MAX_FILENAME + 8];
	snprintf(out_file, sizeof(out_file), "client%d_%s", rank, files[i].file_name);
	FILE *file = fopen(out_file, "w");
	for (int j = 0; j < files[i].total_chunks; j++) {
		fprintf(file, "%.*s\n", HASH_SIZE, files[i].chunks[j].hash);
	}
	fclose(file);
}

void *download_thread_func(void *arg)
{
	// download file from seeds/peers & tracker communication
	int rank = *(int *)arg;

	// send init files data to tracker
	MPI_Send(&nr_files, 1, MPI_INT, TRACKER_RANK, TAG_INFO, MPI_COMM_WORLD);
	for (int i = 0; i < nr_files; i++) {
		MPI_Send(files[i].file_name, MAX_FILENAME, MPI_CHAR, TRACKER_RANK, TAG_INFO, MPI_COMM_WORLD);
		MPI_Send(&files[i].owned_chunks, 1, MPI_INT, TRACKER_RANK, TAG_INFO, MPI_COMM_WORLD);
		for (int j = 0; j < files[i].owned_chunks; j++) {
			MPI_Send(files[i].chunks[j].hash, HASH_SIZE, MPI_CHAR, TRACKER_RANK, TAG_INFO, MPI_COMM_WORLD);
		}
	}

	char ack[3];
	MPI_Status status;
	MPI_Recv(ack, 3, MPI_CHAR, TRACKER_RANK, TAG_INFO, MPI_COMM_WORLD, &status);
	// printf("[Peer %d] Received ACK from tracker\n", rank);

	// request & receive swarm for each file client wants to download
	req_recv_swarm(rank);

	int files_downloaded = 0;
	int i = nr_files;
	while (i < nr_files + nr_req_files) {
		// download chunks and update tracker after 10 chunks downloaded
		int update_time = 0;
		while (update_time % 10 != 0 || update_time == 0) {
			int unowned_chunk = find_unowned_chunk(i);
			if (unowned_chunk != -1) {
				// find client that has the chunk
				int client = find_client(i, unowned_chunk);
				// send request to client
				MPI_Send(files[i].file_name, MAX_FILENAME, MPI_CHAR, client, TAG_CHUNK, MPI_COMM_WORLD);
				MPI_Send(&unowned_chunk, 1, MPI_INT, client, TAG_CHUNK, MPI_COMM_WORLD);
				// printf("[Peer %d] Requested chunk %d of file %s from %d\n", rank, unowned_chunk,
				//  receive chunk from client
				MPI_Recv(files[i].chunks[unowned_chunk].hash, HASH_SIZE, MPI_CHAR, client, TAG_UPLOAD, MPI_COMM_WORLD,
						 &status);
				// printf("[Peer %d] Received chunk %d of file %s from %d\n", rank, unowned_chunk, files[i].file_name,
				files[i].owned_chunks++;
				files[i].chunks[unowned_chunk].own = 1;
				update_time++;
			} else {
				// file downloaded completely
				write_file(rank, i);
				files_downloaded++;
				// printf("CLient %d downld completed file %s. total %d\n", rank, files[i].file_name, files_downloaded);
				i++;
				if (files_downloaded == nr_req_files) {
					break;
				}
			}
		}
		// update swarm from tracker
		MPI_Send("update", 6, MPI_CHAR, TRACKER_RANK, TAG_UPDATE, MPI_COMM_WORLD);
		MPI_Send(&nr_files, 1, MPI_INT, TRACKER_RANK, TAG_UPDATE, MPI_COMM_WORLD);
		for (int k = 0; k < nr_files; k++) {
			MPI_Send(files[k].file_name, MAX_FILENAME, MPI_CHAR, TRACKER_RANK, TAG_UPDATE, MPI_COMM_WORLD);
			MPI_Send(&files[k].owned_chunks, 1, MPI_INT, TRACKER_RANK, TAG_UPDATE, MPI_COMM_WORLD);
			for (int j = 0; j < files[k].owned_chunks; j++) {
				MPI_Send(files[k].chunks[j].hash, HASH_SIZE, MPI_CHAR, TRACKER_RANK, TAG_UPDATE, MPI_COMM_WORLD);
			}
		}
		MPI_Recv(ack, 3, MPI_CHAR, TRACKER_RANK, TAG_UPDATE, MPI_COMM_WORLD, &status);
		for (int z = nr_files; z < nr_files + nr_req_files; z++) {
			MPI_Send(files[z].file_name, MAX_FILENAME, MPI_CHAR, TRACKER_RANK, TAG_SWARM, MPI_COMM_WORLD);
			MPI_Recv(&files[z].swarm_id, 1, MPI_INT, TRACKER_RANK, TAG_SWARM, MPI_COMM_WORLD, &status);
			MPI_Recv(&files[z].total_chunks, 1, MPI_INT, 0, TAG_SWARM, MPI_COMM_WORLD, &status);
			for (int j = 0; j < files[z].swarm_id; j++) {
				MPI_Recv(&files[z].swarm[j].client, 1, MPI_INT, TRACKER_RANK, TAG_SWARM, MPI_COMM_WORLD, &status);
				MPI_Recv(files[z].swarm[j].chunk_id, files[z].total_chunks, MPI_INT, TRACKER_RANK, TAG_SWARM,
						 MPI_COMM_WORLD, &status);
			}
		}
	}
	MPI_Send("FINISHED", 9, MPI_CHAR, TRACKER_RANK, TAG_FINISH, MPI_COMM_WORLD);
	return NULL;
}

void *upload_thread_func(void *arg)
{
	// file segm request response that client own from other client
	int rank = *(int *)arg;

	MPI_Status status;
	while (1) {
		char file_name[MAX_FILENAME];
		MPI_Recv(file_name, MAX_FILENAME, MPI_CHAR, MPI_ANY_SOURCE, TAG_CHUNK, MPI_COMM_WORLD, &status);
		if (strcmp(file_name, "STOP") == 0 && status.MPI_SOURCE == TRACKER_RANK) {
			// printf("Peer %d stopped\n", rank);
			break;
		}
		int chunk_id;
		MPI_Recv(&chunk_id, 1, MPI_INT, status.MPI_SOURCE, TAG_CHUNK, MPI_COMM_WORLD, &status);
		for (int i = 0; i < nr_files; i++) {
			if (strcmp(files[i].file_name, file_name) == 0) {
				MPI_Send(files[i].chunks[chunk_id].hash, HASH_SIZE, MPI_CHAR, status.MPI_SOURCE, TAG_UPLOAD,
						 MPI_COMM_WORLD);
				break;
			}
		}
		// printf("[Peer %d] Sent chunk %d of file %s to %d\n", rank, chunk_id, file_name, status.MPI_SOURCE);
	}
	return NULL;
}

void initialize_tracker(FileInfo *tracker_files, int *nr_tracker_files, int numtasks)
{
	MPI_Status status;
	int client_nfiles, client_nchunks;
	for (int i = 1; i < numtasks; i++) {
		MPI_Recv(&client_nfiles, 1, MPI_INT, i, TAG_INFO, MPI_COMM_WORLD, &status);
		for (int j = 0; j < client_nfiles; j++) {
			char recv_fname[MAX_FILENAME], recv_hash[HASH_SIZE];
			int found = 0;
			MPI_Recv(recv_fname, MAX_FILENAME, MPI_CHAR, i, TAG_INFO, MPI_COMM_WORLD, &status);
			for (int k = 0; k < *nr_tracker_files; k++) {
				if (strcmp(tracker_files[k].file_name, recv_fname) == 0) {
					found = 1;
					tracker_files[k].swarm[tracker_files[k].swarm_id].client = i;
					MPI_Recv(&client_nchunks, 1, MPI_INT, i, TAG_INFO, MPI_COMM_WORLD, &status);
					for (int x = 0; x < client_nchunks; x++) {
						MPI_Recv(recv_hash, HASH_SIZE, MPI_CHAR, i, TAG_INFO, MPI_COMM_WORLD, &status);
						tracker_files[k].swarm[tracker_files[k].swarm_id].chunk_id[x] = 1;
					}
					tracker_files[k].swarm_id++;
				}
			}

			if (found == 0) {
				strcpy(tracker_files[*nr_tracker_files].file_name, recv_fname);
				tracker_files[*nr_tracker_files].swarm_id = 0;
				tracker_files[*nr_tracker_files].swarm[0].client = i;
				MPI_Recv(&client_nchunks, 1, MPI_INT, i, TAG_INFO, MPI_COMM_WORLD, &status);
				tracker_files[*nr_tracker_files].total_chunks = client_nchunks;
				tracker_files[*nr_tracker_files].owned_chunks = client_nchunks;
				for (int x = 0; x < numtasks; x++) {
					for (int l = 0; l < MAX_CHUNKS; l++)
						tracker_files[*nr_tracker_files].swarm[x].chunk_id[l] = 0;
				}
				for (int x = 0; x < client_nchunks; x++) {
					MPI_Recv(recv_hash, HASH_SIZE, MPI_CHAR, i, TAG_INFO, MPI_COMM_WORLD, &status);
					strncpy(tracker_files[*nr_tracker_files].chunks[x].hash, recv_hash, HASH_SIZE);
					tracker_files[*nr_tracker_files].swarm[0].chunk_id[x] = 1;
					tracker_files[*nr_tracker_files].chunks[x].own = 1;
				}
				tracker_files[*nr_tracker_files].swarm_id = 1;
				(*nr_tracker_files)++;
			}
		}
	}
}

void manage_tracker_req(int numtasks, int *nr_tracker_files, FileInfo *tracker_files)
{
	MPI_Status status;
	int client_nfiles, client_nchunks;
	int cnt = 1;

	while (cnt < numtasks) {
		char recv_fname[MAX_FILENAME];
		MPI_Recv(recv_fname, MAX_FILENAME, MPI_CHAR, MPI_ANY_SOURCE, MPI_ANY_TAG, MPI_COMM_WORLD, &status);
		if (status.MPI_TAG == TAG_SWARM) {
			for (int i = 0; i < *nr_tracker_files; i++) {
				if (strcmp(tracker_files[i].file_name, recv_fname) == 0) {
					MPI_Send(&tracker_files[i].swarm_id, 1, MPI_INT, status.MPI_SOURCE, TAG_SWARM, MPI_COMM_WORLD);
					// printf("Sending swarm_id %d\n", tracker_files[i].swarm_id);
					MPI_Send(&tracker_files[i].total_chunks, 1, MPI_INT, status.MPI_SOURCE, TAG_SWARM, MPI_COMM_WORLD);
					for (int j = 0; j < tracker_files[i].swarm_id; j++) {
						MPI_Send(&tracker_files[i].swarm[j].client, 1, MPI_INT, status.MPI_SOURCE, TAG_SWARM,
								 MPI_COMM_WORLD);
						MPI_Send(tracker_files[i].swarm[j].chunk_id, tracker_files[i].total_chunks, MPI_INT,
								 status.MPI_SOURCE, TAG_SWARM, MPI_COMM_WORLD);
					}
					break;
				}
			}
		} else if (status.MPI_TAG == TAG_UPDATE) {
			MPI_Recv(&client_nfiles, 1, MPI_INT, status.MPI_SOURCE, TAG_UPDATE, MPI_COMM_WORLD, &status);
			for (int i = 0; i < client_nfiles; i++) {
				char recv_fname[MAX_FILENAME], recv_hash[HASH_SIZE];
				MPI_Recv(recv_fname, MAX_FILENAME, MPI_CHAR, status.MPI_SOURCE, TAG_UPDATE, MPI_COMM_WORLD, &status);
				for (int j = 0; j < *nr_tracker_files; j++) {
					if (strcmp(tracker_files[j].file_name, recv_fname) == 0) {
						int swarm_id = tracker_files[j].swarm_id;
						for (int k = 0; k < tracker_files[j].swarm_id; k++) {
							if (status.MPI_SOURCE == tracker_files[j].swarm[k].client) {
								swarm_id = k;
								break;
							}
						}
						tracker_files[j].swarm[swarm_id].client = status.MPI_SOURCE;
						MPI_Recv(&client_nchunks, 1, MPI_INT, status.MPI_SOURCE, TAG_UPDATE, MPI_COMM_WORLD, &status);
						for (int x = 0; x < client_nchunks; x++) {
							MPI_Recv(recv_hash, HASH_SIZE, MPI_CHAR, status.MPI_SOURCE, TAG_UPDATE, MPI_COMM_WORLD,
									 &status);
							tracker_files[j].swarm[swarm_id].chunk_id[x] = 1;
						}

						if (tracker_files[j].swarm_id == swarm_id) {
							tracker_files[j].swarm_id++;
						}
						break;
					}
				}
			}
			MPI_Send("ACK", 3, MPI_CHAR, status.MPI_SOURCE, TAG_UPDATE, MPI_COMM_WORLD);
		} else if (status.MPI_TAG == TAG_FINISH) {
			cnt++;
		}
	}
}

void tracker(int numtasks, int rank)
{
	FileInfo tracker_files[MAX_FILES];
	int nr_tracker_files = 0;

	initialize_tracker(tracker_files, &nr_tracker_files, numtasks);

	for (int i = 1; i < numtasks; i++) {
		MPI_Send("ACK", 3, MPI_CHAR, i, TAG_INFO, MPI_COMM_WORLD);
		// printf("ACK sent to %d\n", i);
	}

	manage_tracker_req(numtasks, &nr_tracker_files, tracker_files);

	for (int i = 1; i < numtasks; i++) {
		MPI_Send("STOP", 5, MPI_CHAR, i, TAG_CHUNK, MPI_COMM_WORLD);
		// printf("STOP sent to %d\n", i);
	}
}

void parse_files_data(int rank)
{
	char file_name[MAX_FILENAME];
	snprintf(file_name, sizeof(file_name), "in%d.txt", rank);

	FILE *file = fopen(file_name, "r");
	fscanf(file, "%d", &nr_files);
	for (int i = 0; i < nr_files; i++) {
		fscanf(file, "%s %d", files[i].file_name, &files[i].total_chunks);
		files[i].owned_chunks = files[i].total_chunks;
		for (int j = 0; j < files[i].total_chunks; j++) {
			fscanf(file, "%s", files[i].chunks[j].hash);
			files[i].chunks[j].own = 1;
		}
	}
	fscanf(file, "%d", &nr_req_files);
	for (int j = nr_files; j < nr_files + nr_req_files; j++) {
		fscanf(file, "%s", files[j].file_name);
	}
	fclose(file);
}

void peer(int numtasks, int rank)
{
	parse_files_data(rank);

	pthread_t download_thread;
	pthread_t upload_thread;
	void *status;
	int r;

	r = pthread_create(&download_thread, NULL, download_thread_func, (void *)&rank);
	if (r) {
		printf("Eroare la crearea thread-ului de download\n");
		exit(-1);
	}

	r = pthread_create(&upload_thread, NULL, upload_thread_func, (void *)&rank);
	if (r) {
		printf("Eroare la crearea thread-ului de upload\n");
		exit(-1);
	}

	r = pthread_join(download_thread, &status);
	if (r) {
		printf("Eroare la asteptarea thread-ului de download\n");
		exit(-1);
	}

	r = pthread_join(upload_thread, &status);
	if (r) {
		printf("Eroare la asteptarea thread-ului de upload\n");
		exit(-1);
	}
}

int main(int argc, char *argv[])
{
	int numtasks, rank;

	int provided;
	MPI_Init_thread(&argc, &argv, MPI_THREAD_MULTIPLE, &provided);
	if (provided < MPI_THREAD_MULTIPLE) {
		fprintf(stderr, "MPI nu are suport pentru multi-threading\n");
		exit(-1);
	}
	MPI_Comm_size(MPI_COMM_WORLD, &numtasks);
	MPI_Comm_rank(MPI_COMM_WORLD, &rank);

	if (rank == TRACKER_RANK) {
		tracker(numtasks, rank);
	} else {
		peer(numtasks, rank);
	}
	MPI_Finalize();
}
