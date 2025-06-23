#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <pthread.h>
#include <float.h>

#define MAX_COUNTRIES 256
#define DATE_LENGTH 11
#define BLOCK_SIZE 100  // Tamaño del bloque a procesar por hilo
#define MAX_LINE_SIZE 1024
#define NUM_THREADS 6    // Número de hilos secundarios

// Estructura para almacenar todos los datos compartidos
typedef struct {
    int ids[MAX_COUNTRIES];               // IDs de los países
    char dtTmin[MAX_COUNTRIES][DATE_LENGTH]; // Fechas de temperaturas mínimas
    float Tmin[MAX_COUNTRIES];            // Temperaturas mínimas
    char dtTmax[MAX_COUNTRIES][DATE_LENGTH]; // Fechas de temperaturas máximas
    float Tmax[MAX_COUNTRIES];            // Temperaturas máximas
    int num_countries;                     // Número total de países
    int current_line;                     // Línea actual a leer
    char *data_filename;                  // Nombre del archivo de datos
    pthread_mutex_t mutex;                // Mutex para actualizar temperaturas
    pthread_mutex_t file_mutex;           // Mutex para acceder al archivo
} SharedData;

// Inicializa la matriz de datos
void initializeData(SharedData *data) {
    for (int i = 0; i < MAX_COUNTRIES; i++) {
        data->ids[i] = i + 1;
        data->Tmin[i] = FLT_MAX;
        data->Tmax[i] = -FLT_MAX;
        strcpy(data->dtTmin[i], "0");
        strcpy(data->dtTmax[i], "0");
    }
}

// Lee los IDs de los países
void readCountryIDs(const char *filename, SharedData *data) {
    FILE *file = fopen(filename, "r");
    if (!file) {
        perror("Error al abrir el archivo de IDs");
        exit(1);
    }

    int index = 0;
    while (fscanf(file, "%d\n", &data->ids[index]) == 1 && index < MAX_COUNTRIES) {
        index++;
    }
    data->num_countries = index; // Actualizar número de países
    fclose(file);
}

// Procesa un bloque de líneas del archivo
void processBlock(char **lines, int num_lines, SharedData *data) {
    for (int i = 0; i < num_lines; i++) {
        char date[DATE_LENGTH];
        float temperature;
        int countryID;

        if (sscanf(lines[i], "%10[^,],%f,%*[^,],%*[^,],%*[^,],%d", date, &temperature, &countryID) == 3) {
            if (countryID > 0 && countryID <= data->num_countries) {
                int index = countryID - 1;

                // Bloquear el mutex antes de actualizar
                pthread_mutex_lock(&data->mutex);

                if (temperature < data->Tmin[index]) {
                    data->Tmin[index] = temperature;
                    strcpy(data->dtTmin[index], date);
                }
                if (temperature > data->Tmax[index]) {
                    data->Tmax[index] = temperature;
                    strcpy(data->dtTmax[index], date);
                }

                // Desbloquear el mutex después de actualizar
                pthread_mutex_unlock(&data->mutex);
            }
        }
    }
}

void *threadFunc(void *arg) {
    SharedData *data = (SharedData *)arg;
    FILE *file;

    // Usar el mutex para acceder al archivo
    pthread_mutex_lock(&data->file_mutex);
    file = fopen(data->data_filename, "r");
    if (!file) {
        perror("Error al abrir el archivo de datos");
        pthread_mutex_unlock(&data->file_mutex);
        pthread_exit(NULL);
    }
    pthread_mutex_unlock(&data->file_mutex);

    char **lines = malloc(BLOCK_SIZE * sizeof(char *));
    for (int i = 0; i < BLOCK_SIZE; i++) {
        lines[i] = malloc(MAX_LINE_SIZE);
    }

    while (1) {
	    // Bloquear el mutex para actualizar la línea actual
	    pthread_mutex_lock(&data->file_mutex);
	    int line_index = data->current_line;
	    if (line_index < 0) {
		pthread_mutex_unlock(&data->file_mutex);
		break; // No hay más líneas
	    }

	    // Leer un bloque de líneas desde el archivo
	    int lines_read = 0;
	    for (int i = 0; i < BLOCK_SIZE; i++) {
		if (fgets(lines[i], MAX_LINE_SIZE, file) == NULL) {
		    // EOF alcanzado
		    data->current_line = -1; // Marcar que no hay más líneas
		    break;
		}
		lines_read++;
	    }

	    // Actualizar el índice global si se leyeron líneas
	    if (lines_read > 0) {
		data->current_line += lines_read;
	    }
	    pthread_mutex_unlock(&data->file_mutex);

	    // Procesar el bloque de líneas leídas
	    if (lines_read > 0) {
		processBlock(lines, lines_read, data);
	    } else {
		break; // No se pudieron leer líneas (EOF o error)
	    }
    }
    

    // Liberar recursos
    for (int i = 0; i < BLOCK_SIZE; i++) {
        free(lines[i]);
    }
    free(lines);
    
    fclose(file);
    pthread_exit(NULL);
}


// Función principal
int main(int argc, char *argv[]) {
    if (argc != 3) {
        fprintf(stderr, "Uso: %s IDs.csv data.csv\n", argv[0]);
        return 1;
    }

    SharedData data;
    initializeData(&data);
    readCountryIDs(argv[1], &data);

    data.data_filename = argv[2]; // Asignar el nombre del archivo de datos
    data.current_line = 0;        // Inicializar línea actual

    // Inicializar los mutex
    pthread_mutex_init(&data.mutex, NULL);
    pthread_mutex_init(&data.file_mutex, NULL);

    pthread_t threads[NUM_THREADS];
    for (int i = 0; i < NUM_THREADS; i++) {
        pthread_create(&threads[i], NULL, threadFunc, &data);
    }

    // Esperar a que terminen los hilos
    for (int i = 0; i < NUM_THREADS; i++) {
        pthread_join(threads[i], NULL);
    }
      
    
        FILE *output = fopen("salida_hilos_aux.txt", "w");
	if (!output) {
	    perror("Error al abrir el archivo de salida");
	    return 1;
	}

	fprintf(output, "Resultados finales:\n");
	fprintf(output, "CountryID   dtTmin     Tmin   dtTmax    Tmax\n");

	for (int i = 0; i < data.num_countries; i++) {
	    if (data.Tmin[i] != FLT_MAX || data.Tmax[i] != -FLT_MAX) {
		fprintf(output, "%03d        %s   %.2f   %s   %.2f\n",
		        data.ids[i],
		        data.dtTmin[i],
		        data.Tmin[i],
		        data.dtTmax[i],
		        data.Tmax[i]);
	    }
	}

	fclose(output);

    
    
    // Destruir los mutex
    pthread_mutex_destroy(&data.mutex);
    pthread_mutex_destroy(&data.file_mutex);
    return 0;
}

