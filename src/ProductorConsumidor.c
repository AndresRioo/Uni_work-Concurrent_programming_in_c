#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <pthread.h>
#include <float.h>

#define MAX_COUNTRIES 256
#define DATE_LENGTH 11

#define BLOCK_SIZE 1000000 // Número de líneas procesadas por bloque
#define MAX_LINE_SIZE 1024
#define N 1024


#define B 100 //150
#define N_threads 4



// Inicializa la matriz de datos ( SE LLAMA EN EL PRINCIPAL ANTES DE NADA )
void initializeData(int ids[], char dtTmin[][DATE_LENGTH], float Tmin[], char dtTmax[][DATE_LENGTH], float Tmax[], int size) {
    for (int i = 0; i < size; i++) {
        ids[i] = i + 1;
        Tmin[i] = FLT_MAX;
        Tmax[i] = -FLT_MAX;
        strcpy(dtTmin[i], "0");
        strcpy(dtTmax[i], "0");
    }
}
// Lee los IDs de los países ( SE LLAMA EN EL PRINCIPAL ANTES DE NADA )
void readCountryIDs(const char *filename, int ids[], int *size) {
    FILE *file = fopen(filename, "r");
    if (!file) {
        perror("Error al abrir el archivo de IDs");
        exit(1);
    }

    int index = 0;
    while (fscanf(file, "%d\n", &ids[index]) == 1 && index < MAX_COUNTRIES) {
        index++;
    }
    *size = index;
    fclose(file);
}

int lineas_procesadas = 0;

void processBlock(char **lines, int num_lines, int ids[], char dtTmin[][DATE_LENGTH], float Tmin[], char dtTmax[][DATE_LENGTH], float Tmax[], int num_countries, pthread_mutex_t *mutex) {
    for (int i = 0; i < num_lines; i++) {
        char date[DATE_LENGTH];
        float temperature;
        int countryID;
        
        lineas_procesadas++;

        // Parsear la línea para extraer la fecha, temperatura y el ID del país
        if (sscanf(lines[i], "%10[^,],%f,%*[^,],%*[^,],%*[^,],%d", date, &temperature, &countryID) == 3) {
            // Verificar que el ID del país esté en el rango válido
            if (countryID > 0 && countryID <= num_countries) {
                int index = countryID - 1; // Ajustar el índice para el arreglo de temperaturas

                // Bloquear el mutex antes de modificar datos compartidos
                pthread_mutex_lock(mutex);

                // Actualizar las temperaturas mínimas y máximas
                if (temperature < Tmin[index]) {
                    Tmin[index] = temperature;
                    strcpy(dtTmin[index], date); // Guardar la fecha correspondiente
                }
                if (temperature > Tmax[index]) {
                    Tmax[index] = temperature;
                    strcpy(dtTmax[index], date); // Guardar la fecha correspondiente
                }

                // Desbloquear el mutex después de modificar datos compartidos
                pthread_mutex_unlock(mutex);
            }
        }
    }
}








// Estructura del búfer circular
typedef struct {
    char ***blocks;       // Puntero a un arreglo de bloques dinámicos
    int in;               // Índice para agregar un nuevo bloque
    int out;              // Índice para consumir un bloque
    int count;            // Número de bloques llenos en el búfer
    pthread_mutex_t mutex;  // Mutex para sincronización
    pthread_cond_t notEmpty; // Condición: hay datos para consumir
    pthread_cond_t notFull;  // Condición: hay espacio para producir
} Buffer;



// Inicializa el búfer con memoria dinámica
void initializeBuffer(Buffer *buffer) {
    buffer->in = 0;
    buffer->out = 0;
    buffer->count = 0;
    pthread_mutex_init(&buffer->mutex, NULL);
    pthread_cond_init(&buffer->notEmpty, NULL);
    pthread_cond_init(&buffer->notFull, NULL);

    // Asignar memoria para los bloques
    buffer->blocks = malloc(B * sizeof(char **));
    if (!buffer->blocks) {
        perror("Error al asignar memoria para los bloques");
        exit(EXIT_FAILURE);
    }

    for (int i = 0; i < B; i++) {
        buffer->blocks[i] = malloc(N * sizeof(char *)); // Asignar memoria para las líneas
        if (!buffer->blocks[i]) {
            perror("Error al asignar memoria para las líneas en un bloque");
            exit(EXIT_FAILURE);
        }
        for (int j = 0; j < N; j++) {
            buffer->blocks[i][j] = malloc(MAX_LINE_SIZE * sizeof(char)); // Asignar memoria para cada línea
            if (!buffer->blocks[i][j]) {
                perror("Error al asignar memoria para una línea");
                exit(EXIT_FAILURE);
            }
        }
    }
}



// Función para destruir el búfer circular
void destroyBuffer(Buffer *buffer) {
    if (buffer != NULL) {
        return; // Si el búfer es NULL, no hacemos nada
    }

    // Liberar los bloques
    for (int i = 0; i < B; i++) { // Iterar sobre todos los bloques
        char **block = buffer->blocks[i];
        if (block != NULL) {
            for (int j = 0; j < N; j++) { // Liberar cada línea en el bloque
                free(block[j]); // Liberar cada línea
            }
            free(block); // Liberar el bloque en sí
        }
    }

    free(buffer->blocks); // Liberar el puntero al arreglo de bloques

    // Destruir el mutex y las variables de condición
    pthread_mutex_destroy(&buffer->mutex);
    pthread_cond_destroy(&buffer->notEmpty);
    pthread_cond_destroy(&buffer->notFull);

    // Liberar el búfer en sí
    free(buffer);
}



















// Indicador global para finalizar
int done = 0; 

void *producer(void *args) {
    // Descomponer los argumentos pasados al hilo
    FILE *file = (FILE *)((void **)args)[0]; // puntero al archivo
    int *ids = (int *)((void **)args)[1]; // puntero a los IDs de países
    Buffer *buffer = (Buffer *)((void **)args)[2]; // puntero al búfer

    // Crear un bloque para almacenar N líneas
    char **cell_producer = malloc(N * sizeof(char *)); // Reserva memoria para el bloque de líneas
    for (int i = 0; i < N; i++) {
        cell_producer[i] = malloc(MAX_LINE_SIZE * sizeof(char)); // Reserva memoria para cada línea
    }

    int line_count = 0; // Contador de líneas leídas
    int total_lines = 0;

    while (1) {
        pthread_mutex_lock(&buffer->mutex); // Bloquear el mutex para acceso seguro

        // Esperar hasta que haya espacio en el búfer
        while (buffer->count == B) {
            pthread_cond_wait(&buffer->notFull, &buffer->mutex);
        }

        // Leer hasta N líneas del archivo
        line_count = 0; // Reiniciar contador de líneas leídas
        for (int i = 0; i < N; i++) {
            if (fgets(cell_producer[i], MAX_LINE_SIZE, file) == NULL) {
                // Si no se puede leer más líneas, salimos del bucle
                break;
            }
            line_count++; // Incrementar el número de líneas leídas
        }

        // Si se han leído líneas, las almacenamos en el búfer
        if (line_count > 0) {
            // Intercambiar el bloque de productor con el bloque del búfer
            char **tmp = buffer->blocks[buffer->in]; // Guardar el bloque actual en un temporal
            buffer->blocks[buffer->in] = cell_producer; // Transferir el bloque del productor al búfer
            cell_producer = tmp; // El bloque temporal se convierte en el nuevo bloque de productor

            buffer->in = (buffer->in + 1) % B; // Actualizar el índice de entrada
            buffer->count++; // Incrementar el número de bloques llenos
            total_lines += line_count;
        }

        pthread_cond_signal(&buffer->notEmpty); // Notificar a los consumidores
        pthread_mutex_unlock(&buffer->mutex); // Desbloquear el mutex

        // Si no se han leído líneas, hemos llegado al final del archivo
        if (line_count < N) {
            break; // Salir del bucle si no hay más líneas para leer
        }
    }

    // Marcar que no habrá más datos
    pthread_mutex_lock(&buffer->mutex);
    done = 1; // Establecer que el productor ha terminado
    pthread_cond_broadcast(&buffer->notEmpty); // Notificar a todos los consumidores
    pthread_mutex_unlock(&buffer->mutex);

    printf("\n\nAcaba un hilo de producer. Líneas leídas: %d\n\n", total_lines);

    // Liberar la memoria de las líneas del productor
    for (int i = 0; i < N; i++) {
        free(cell_producer[i]); // Liberar cada línea
    }
    free(cell_producer); // Liberar el bloque de líneas

    return NULL; // Terminar el hilo
}




void *consumer(void *args) {
    pthread_t thread_id = pthread_self();

    // Descomponer los argumentos pasados al hilo
    int *ids = (int *)((void **)args)[1]; // puntero a los IDs de países
    char (*dtTmin)[DATE_LENGTH] = (char (*)[DATE_LENGTH])((void **)args)[2]; // matriz para fechas mínimas
    float *Tmin = (float *)((void **)args)[3]; // puntero al array de temperaturas mínimas
    char (*dtTmax)[DATE_LENGTH] = (char (*)[DATE_LENGTH])((void **)args)[4]; // matriz para fechas máximas
    float *Tmax = (float *)((void **)args)[5]; // puntero al array de temperaturas máximas
    int num_countries = *(int *)((void **)args)[6]; // Número de países
    Buffer *buffer = (Buffer *)((void **)args)[7]; // puntero al búfer
    pthread_mutex_t *mutexTemperaturas = (pthread_mutex_t *)((void **)args)[8]; // Leer el segundo mutex (posición 8)

    // Variables para almacenar bloques de líneas
    char **block_lines = malloc(N * sizeof(char *)); // Array dinámico para almacenar líneas del bloque
    if (block_lines == NULL) {
        perror("Error al reservar memoria para block_lines");
        return NULL; // Manejo de error en la asignación de memoria
    }

    int line_count = 0; // Contador de líneas procesadas

    while (1) {
        pthread_mutex_lock(&buffer->mutex); // Bloquear el mutex para acceso seguro

        // Esperar hasta que haya datos disponibles en el búfer
        while (buffer->count == 0) {
            if (done) {
                // Si el productor ha terminado y no hay más bloques en el búfer
                pthread_mutex_unlock(&buffer->mutex);
                printf("Consumer (Thread ID: %lu) finaliza. Líneas procesadas: %d\n", thread_id, line_count);
                free(block_lines); // Liberar memoria antes de salir
                return NULL; // Terminar el hilo si no hay más datos
            }

            pthread_cond_wait(&buffer->notEmpty, &buffer->mutex);
        }

        // Procesar un bloque completo
        char **block = buffer->blocks[buffer->out];
        int num_lines = N; // Se procesará un bloque completo
        for (int i = 0; i < num_lines; i++) {
            if (block[i] == NULL) { // Verificar bloque de finalización
                num_lines = i; // Ajustar el número de líneas si se recibe un bloque de finalización
                break; // Salir del bucle si se encuentra el bloque de finalización
            }
            block_lines[i] = strdup(block[i]); // Copia la línea del bloque
            if (block_lines[i] == NULL) {
                perror("Error al duplicar la línea");
                num_lines = i; // Ajustar el número de líneas si falla
                break;
            }
        }

        buffer->out = (buffer->out + 1) % B; // Actualizar el índice de salida
        buffer->count--; // Decrementar el número de bloques llenos

        // Notificar a los productores que hay espacio disponible
        pthread_cond_signal(&buffer->notFull);
        pthread_mutex_unlock(&buffer->mutex); // Desbloquear el mutex

        // Procesar el bloque de líneas
        processBlock(block_lines, num_lines, ids, dtTmin, Tmin, dtTmax, Tmax, num_countries, mutexTemperaturas);

        // Liberar la memoria del bloque consumido
        for (int i = 0; i < num_lines; i++) {
            free(block_lines[i]); // Liberar cada línea
        }

        // Incrementar el contador de líneas procesadas
        line_count += num_lines;
    }

    printf("Acaba un hilo de consumer (Thread ID: %lu). Líneas procesadas: %d\n", thread_id, line_count);
    free(block_lines); // Liberar memoria al finalizar
    return NULL; // Terminar el hilo
}




void *producer2(void *args) {
    // Descomponer los argumentos pasados al hilo
    FILE *file = (FILE *)((void **)args)[0]; // puntero al archivo
    int *ids = (int *)((void **)args)[1];   // puntero a los IDs de países
    Buffer *buffer = (Buffer *)((void **)args)[2]; // puntero al búfer

    // Declarar un bloque para almacenar N líneas
    char **lines = malloc(N * sizeof(char *)); // Reserva memoria para el bloque de líneas
    for (int i = 0; i < N; i++) {
        lines[i] = malloc(MAX_LINE_SIZE * sizeof(char)); // Reserva memoria para cada línea
    }

    int line_count = 0; // Contador de líneas leídas
    int total_lines = 0;

    while (1) {
        pthread_mutex_lock(&buffer->mutex); // Bloquear el mutex para acceso seguro

        // Esperar hasta que haya espacio en el búfer
        while (buffer->count == B) {
            pthread_cond_wait(&buffer->notFull, &buffer->mutex);
        }

        // Leer hasta N líneas del archivo
        line_count = 0; // Reiniciar contador de líneas leídas

        for (int i = 0; i < N; i++) {
            if (fgets(lines[i], MAX_LINE_SIZE, file) == NULL) {
                // Si no se puede leer más líneas, salimos del bucle
                break;
            }
            line_count++; // Incrementar el número de líneas leídas
        }

        // Si se han leído líneas, las almacenamos en el búfer
        if (line_count > 0) {
            buffer->blocks[buffer->in] = lines; // Asignar el bloque actual al búfer
            buffer->in = (buffer->in + 1) % B; // Actualizar el índice de entrada
            buffer->count++; // Incrementar el número de bloques llenos
            total_lines += line_count;
        }

        pthread_cond_signal(&buffer->notEmpty); // Notificar a los consumidores
        pthread_mutex_unlock(&buffer->mutex); // Desbloquear el mutex

        // Si no se han leído líneas, hemos llegado al final del archivo
        //if (line_count < N) {
          //  break; // Salir del bucle si no hay más líneas para leer
        //}
        
        if (line_count == 0) {
            break; // Salir del bucle si no hay más líneas para leer
        }
    }

    // Marcar que no habrá más datos
    pthread_mutex_lock(&buffer->mutex);
    done = 1; // Establecer que el productor ha terminado
    pthread_cond_broadcast(&buffer->notEmpty); // Notificar a todos los consumidores
    pthread_mutex_unlock(&buffer->mutex);

    printf("\n\nAcaba un hilo de producer. Líneas leídas: %d\n\n", total_lines);

    // Liberar la memoria de las líneas
    for (int i = 0; i < N; i++) {
        //free(lines[i]); // Liberar cada línea
    }
    //free(lines); // Liberar el bloque de líneas

    return NULL; // Terminar el hilo
}


void *consumer2(void *args) {
    pthread_t thread_id = pthread_self();

    // Descomponer los argumentos pasados al hilo
    int *ids = (int *)((void **)args)[1]; // puntero a los IDs de países
    char (*dtTmin)[DATE_LENGTH] = (char (*)[DATE_LENGTH])((void **)args)[2]; // matriz para fechas mínimas
    float *Tmin = (float *)((void **)args)[3]; // puntero al array de temperaturas mínimas
    char (*dtTmax)[DATE_LENGTH] = (char (*)[DATE_LENGTH])((void **)args)[4]; // matriz para fechas máximas
    float *Tmax = (float *)((void **)args)[5]; // puntero al array de temperaturas máximas
    int num_countries = *(int *)((void **)args)[6]; // Número de países
    Buffer *buffer = (Buffer *)((void **)args)[7]; // puntero al búfer
    pthread_mutex_t *mutexTemperaturas = (pthread_mutex_t *)((void **)args)[8]; // Leer el segundo mutex (posición 8)

    // Variables para almacenar bloques de líneas
    char **block_lines = malloc(N * sizeof(char *)); // Array dinámico para almacenar líneas del bloque
    if (block_lines == NULL) {
        perror("Error al reservar memoria para block_lines");
        return NULL; // Manejo de error en la asignación de memoria
    }

    int line_count = 0; // Contador de líneas procesadas

    while (1) {
        pthread_mutex_lock(&buffer->mutex); // Bloquear el mutex para acceso seguro

        // Esperar hasta que haya datos disponibles en el búfer
        while (buffer->count == 0) {
            if (done && buffer->count == 0) {
                // Si el productor ha terminado y no hay más bloques en el búfer
                pthread_mutex_unlock(&buffer->mutex);
                printf("Consumer (Thread ID: %lu) finaliza. Líneas procesadas: %d\n", thread_id, line_count);
                free(block_lines); // Liberar memoria antes de salir
                return NULL; // Terminar el hilo si no hay más datos
            } 

            pthread_cond_wait(&buffer->notEmpty, &buffer->mutex);
        }


        int num_lines = N; // Se procesará un bloque completo
        for (int i = 0; i < N; i++) {
            if (buffer->blocks[buffer->out][i] == NULL) { // Verificar bloque de finalización
                num_lines = i; // Ajustar el número de líneas si se recibe un bloque de finalización
                break; // Salir del bucle si se encuentra el bloque de finalización
            }
            block_lines[i] = strdup(buffer->blocks[buffer->out][i]); // Copia la línea del bloque
            if (block_lines[i] == NULL) {
                perror("Error al duplicar la línea");
                num_lines = i; // Ajustar el número de líneas si falla
                break;
            }
        }

        buffer->out = (buffer->out + 1) % B; // Actualizar el índice de salida
        buffer->count--; // Decrementar el número de bloques llenos

        // Notificar a los productores que hay espacio disponible
        pthread_cond_signal(&buffer->notFull);
        pthread_mutex_unlock(&buffer->mutex); // Desbloquear el mutex

        // Procesar el bloque de líneas
        processBlock(block_lines, num_lines, ids, dtTmin, Tmin, dtTmax, Tmax, num_countries, mutexTemperaturas);

        // Liberar la memoria del bloque consumido
        for (int i = 0; i < num_lines; i++) {
            free(block_lines[i]); // Liberar cada línea
        }

        // Incrementar el contador de líneas procesadas
        line_count += num_lines;
        
    }

    printf("1. Acaba un hilo de consumer (Thread ID: %lu). Líneas procesadas: %d\n", thread_id, line_count);
    free(block_lines); // Liberar memoria al finalizar
    return NULL; // Terminar el hilo
}





int main(int argc, char *argv[]) {


    if (argc != 3) {
        fprintf(stderr, "Uso: %s IDs.csv Data.csv\n", argv[0]);
        return 1;
    }
    
    const char *idsFile = argv[1];
    const char *dataFile = argv[2];

    int ids[MAX_COUNTRIES];
    char dtTmin[MAX_COUNTRIES][DATE_LENGTH];
    float Tmin[MAX_COUNTRIES];
    char dtTmax[MAX_COUNTRIES][DATE_LENGTH];
    float Tmax[MAX_COUNTRIES];
    
    
    pthread_t threads[N_threads + 1 ]; // crear N hilos (2) + 1 producer (3)
    void *threadArgs[9];  // array con los parametros de los hilos
    pthread_mutex_t mutexTemperaturas; // llave de hilo


    pthread_mutex_init(&mutexTemperaturas, NULL);   // el hilo tiene acceso a secciones críticas 

    int num_countries;
    readCountryIDs(idsFile, ids, &num_countries); // guardar los idsd y el numero de countries 
    
    
    initializeData(ids, dtTmin, Tmin, dtTmax, Tmax, num_countries);  // iniciar los datos 

    FILE *file = fopen(dataFile, "r");
    if (!file) {
        perror("Error al abrir el archivo de datos");
        return 1;
    }

    char header[1024];
    fgets(header, sizeof(header), file); // descartar encabezado del archivo

    threadArgs[0] = file;
    threadArgs[1] = ids;
    threadArgs[2] = dtTmin;
    threadArgs[3] = Tmin;
    threadArgs[4] = dtTmax;
    threadArgs[5] = Tmax;
    threadArgs[6] = &num_countries;
    threadArgs[8] = &mutexTemperaturas;


    // Declarar e inicializar el búfer
    Buffer buffer;
    initializeBuffer(&buffer);

    threadArgs[7] = &buffer;


    for (int i = 0; i < N_threads; i++) {
        pthread_create(&threads[i], NULL, consumer, threadArgs);
        // se inicializan ambos hilos ( ambos tienen mismos parametros , si edita uno , se edita el otro )
    	// tiene su propia función + argumentos
    }

    // Llama al productor para llenar el búfer
    void *threadArgs2[3];
    
    threadArgs2[0] = file;
    threadArgs2[1] = ids;
    threadArgs2[2] = &buffer;
    

    pthread_create(&threads[2],NULL,producer,threadArgs2);



    for (int i = 0; i < N_threads + 1; i++) {
        pthread_join(threads[i], NULL);  // espera a ambos hilos
    }


    printf("\nSalimos de la barrera, Guardamos información en salida_hilos_prod_serv.txt\n");
    

    FILE *output = fopen("salida_hilos_prod_serv.txt", "w");
    if (!output) {
        perror("Error al abrir el archivo de salida");
        return 1;
    }

    fprintf(output, "Resultados finales:\n");
    fprintf(output, "CountryID   dtTmin     Tmin   dtTmax    Tmax\n");
    for (int i = 0; i < num_countries; i++) {
        if (Tmin[i] != FLT_MAX || Tmax[i] != -FLT_MAX) {
            fprintf(output, "%03d        %s   %.2f   %s   %.2f\n",
                    ids[i],
                    dtTmin[i],
                    Tmin[i],
                    dtTmax[i],
                    Tmax[i]);
        }
    }

    fclose(output);


    // Liberar recursos al final
    fclose(file);
    destroyBuffer(&buffer);
    
    printf("Memoria liberada\n");
    
    
    return 0;
}







