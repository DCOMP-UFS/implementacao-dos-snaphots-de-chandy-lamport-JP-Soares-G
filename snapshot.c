/* File:
 *    parte4.c
 *
 *
 *
 * Compile:  mpicc -g -Wall -o snapshot snapshot.c -lpthread -lrt
 * Usage:    mpiexec -n 3 ./snapshot
 */

// Fazer com o snapshot comece do init snapshot, ouvir enquanto não recebe outro marcador e refatorar codigo

#include <stdio.h>
#include <string.h>
#include <stdlib.h>
#include <pthread.h>
#include <unistd.h>
#include <semaphore.h>
#include <time.h>
#include <mpi.h>

#define BUFFER_SIZE 6 // Númermo máximo de tarefas enfileiradas

typedef struct Clock
{
    int p[3];
} Clock;

Clock clockMain = {{0, 0, 0}};

typedef struct msg
{
    int remetente;
    int destinatario;
    Clock clock;
} Msg;

typedef struct listSnaps
{
    Clock valor;
    struct listSnaps *proximo;
} ListSnaps;

typedef struct Snapshot
{
    Clock copiaClock;
    ListSnaps *mensagens[3];
} Snapshot;

typedef struct canal
{
    int canal;
    int jaFoiEnviadoPara;
} Canal;

Snapshot snapshot;

int foiIniciadoSnapshot = 0;
int processoIniciador = -1;

Canal canais[3];
int canaisCount = 0;
int canaisMarcadores[3];
int canaisMarcadoresCounter = 0;

Clock qEntrada[BUFFER_SIZE];
Msg qSaida[BUFFER_SIZE];

int qEntradaCount = 0;
int qSaidaCount = 0;

pthread_mutex_t mutexEntrada;
pthread_mutex_t mutexSaida;
pthread_mutex_t mutexSnapshot;

pthread_cond_t condFullEntrada;
pthread_cond_t condEmptyEntrada;

pthread_cond_t condFullSaida;
pthread_cond_t condEmptySaida;

pthread_cond_t condMsgFila;

void printarCanais()
{
    printf("Canais: ");
    for (int i = 0; i < canaisCount; i++)
    {
        printf("%d ", canais[i].canal);
    }
    printf("\n");
}

void printarRelogio(Clock *clock, int pid, int action)
{
    switch (action)
    {
    case 1:
        printf("[EVENT]     Processo: %d | Relogio: (%d, %d, %d)\n", pid, clock->p[0], clock->p[1], clock->p[2]);
        break;
    case 2:
        printf("[SEND]      Processo: %d | Relogio: (%d, %d, %d)\n", pid, clock->p[0], clock->p[1], clock->p[2]);
        break;
    case 3:
        printf("[RECEIVE]   Processo: %d | Relogio: (%d, %d, %d)\n", pid, clock->p[0], clock->p[1], clock->p[2]);
        break;
    default:
        break;
    }
}

void printarListaCanalComunicacao(ListSnaps *head, int canal, int pid)
{
    ListSnaps *atual = head;
    printf("[SNAPSHOT]  Processo: %d | Canal %d: ", pid, canal);
    while (atual != NULL)
    {

        printf("(%d, %d, %d) ", atual->valor.p[0], atual->valor.p[1], atual->valor.p[2]);
        atual = atual->proximo;
    }
    printf("\n");
}

void adicionaCanais(int source, int jaFoiEnviado)
{
    int jaExiste = 0;
    for (int i = 0; i < canaisCount; i++)
    {
        if (source == canais[i].canal)
        {
            jaExiste = 1;
            break;
        }
    }
    if (jaExiste == 0)
    {
        canais[canaisCount].canal = source;
        canais[canaisCount].jaFoiEnviadoPara = jaFoiEnviado;
        canaisCount++;
    }
}

// adiciona a lista encadeada do snapshot
void salvarCanalComunicacao(int remetente, Clock clock)
{

    ListSnaps *item = NULL;
    item = (ListSnaps *)malloc(sizeof(ListSnaps *));
    item->valor = clock;

    if (snapshot.mensagens[remetente] == NULL)
    {
        snapshot.mensagens[remetente] = item;
        return;
    }

    item->proximo = snapshot.mensagens[remetente];
    snapshot.mensagens[remetente] = item;
}

void enviaMarcadoresCanais(int pid)
{
    // printf("Processo %d, Enviando Marcadores para os Canais de Saida\n", pid);
    int foiEnviado = 0;
    for (int i = 0; i < canaisCount; i++)
    {

        if (!canais[i].jaFoiEnviadoPara && canais[i].canal != processoIniciador)
        {
            sendSnapshotMarker(pid, canais[i].canal);
            canais[i].jaFoiEnviadoPara = 1;
            foiEnviado = 1;
        }
    }

    // if(foiEnviado) printf("Processo %d, Enviando Marcadores para os Canais de Saida\n", pid);
}

void Event(int pid, Clock *clock)
{
    pthread_mutex_lock(&mutexSnapshot);
    clock->p[pid]++;
    printarRelogio(clock, pid, 1);
    pthread_mutex_unlock(&mutexSnapshot);
}

void Send(int remetente, int destinatario, Clock *clockPID)
{
    pthread_mutex_lock(&mutexSaida);

    pthread_mutex_lock(&mutexSnapshot);

    clockPID->p[remetente]++;
    printarRelogio(clockPID, remetente, 2);

    pthread_mutex_unlock(&mutexSnapshot);

    while (qSaidaCount == BUFFER_SIZE)
    { // enquanto a fila de envio estiver cheia, libera o mutex de saida para as outras threads
        pthread_cond_wait(&condFullSaida, &mutexSaida);
    }

    Msg *msg = (Msg *)malloc(sizeof(Msg));
    msg->clock = (*clockPID);
    msg->remetente = remetente;
    msg->destinatario = destinatario;

    qSaida[qSaidaCount] = *msg;
    qSaidaCount++;

    pthread_mutex_unlock(&mutexSaida);
    pthread_cond_signal(&condEmptySaida); // desbloqueia a condicional
}

void Receive(int pid, Clock *clockPID)
{

    pthread_mutex_lock(&mutexEntrada);

    while (qEntradaCount == 0)
    { // Enquanto a fila estiver vazia, libera o mutex
        pthread_cond_wait(&condEmptyEntrada, &mutexEntrada);
    }

    Clock clock = qEntrada[0]; // Consome do inicio da fila

    for (int i = 0; i < qEntradaCount - 1; i++)
    {
        qEntrada[i] = qEntrada[i + 1];
    }
    qEntradaCount--; // consome o relogio

    pthread_mutex_lock(&mutexSnapshot); // bloqueia o mutex do snapshot
    clockPID->p[pid]++;

    for (int i = 0; i < 3; i++)
    {
        if (clock.p[i] > clockPID->p[i])
        {
            clockPID->p[i] = clock.p[i];
        }
    }
    printarRelogio(clockPID, pid, 3);
    pthread_mutex_unlock(&mutexSnapshot); // libera o mutex do snapshot

    pthread_cond_signal(&condMsgFila);
    pthread_mutex_unlock(&mutexEntrada);
    pthread_cond_signal(&condFullEntrada);
}

void fazerSnapshot(int pid){
    

        if (!foiIniciadoSnapshot)
        {

            while (qEntradaCount > 0)
            {
                pthread_cond_wait(&condMsgFila, &mutexSnapshot);
            }

            printf("[SNAPSHOT]  Processo: %d | Marcador Recebido\n", pid);
            snapshot.copiaClock = clockMain;
            printf("[SNAPSHOT]  Processo: %d | Copia Local (%d, %d, %d)\n", pid, snapshot.copiaClock.p[0], snapshot.copiaClock.p[1], snapshot.copiaClock.p[2]);
        }
        
        foiIniciadoSnapshot = 1;
        enviaMarcadoresCanais(pid);

        
}

void getEntrada(int pid)
{

    MPI_Status status;
    Clock clock;
    MPI_Recv(&clock, 3, MPI_INT, MPI_ANY_SOURCE, MPI_ANY_TAG, MPI_COMM_WORLD, &status);

    int source = status.MPI_SOURCE;
    if(source != processoIniciador) {
        adicionaCanais(source, 0);
    }
    

    if (clock.p[0] == -1) // se receber um marcador 
    {
        pthread_mutex_lock(&mutexSnapshot);
        fazerSnapshot(pid); // realiza o snapshot
        pthread_mutex_unlock(&mutexSnapshot);
        return;
    }else {
        
    }

    if (foiIniciadoSnapshot)
    {
        salvarCanalComunicacao(source, clock); // salva as mensagens recebidas pela canal de comunicação
    }

    pthread_mutex_lock(&mutexEntrada); 

    while (qEntradaCount == BUFFER_SIZE)
    {
        pthread_cond_wait(&condFullEntrada, &mutexEntrada);
    }

    qEntrada[qEntradaCount] = clock;
    qEntradaCount++;

    pthread_mutex_unlock(&mutexEntrada); // desbloqueia
    pthread_cond_signal(&condEmptyEntrada);
}

void submitSaida()
{
    // printf("Mutex desbloqueado\n");
    pthread_mutex_lock(&mutexSaida); // bloqueia o mutex de saida;
                                     // printf("Mutex bloqueado\n");
    while (qSaidaCount == 0)
    {
        pthread_cond_wait(&condEmptySaida, &mutexSaida); // enquanto a fila de saida estiver vazia, o mutex é liberado para as outras threads
    }

    int *resultados;
    resultados = calloc(3, sizeof(int));
    // Consome da fila de saída
    Msg resposta = qSaida[0];
    for (int i = 0; i < qSaidaCount - 1; i++)
    {
        qSaida[i] = qSaida[i + 1];
    }
    qSaidaCount--;
    //   printf("qSaidaCount = %d\n", qSaidaCount);
    for (int i = 0; i < 3; i++)
    {
        resultados[i] = resposta.clock.p[i];
    }
    // inicialização do snapshot
    if (resposta.destinatario == -1)
    {
        int n = 3;
        for (int i = 0; i < n; i++)
        {
            if (i == resposta.remetente)
                continue;
            adicionaCanais(i, 1);
            MPI_Send(resultados, 3, MPI_INT, i, resposta.remetente, MPI_COMM_WORLD);
        }
    }
    else
    {
        MPI_Send(resultados, 3, MPI_INT, resposta.destinatario, resposta.remetente, MPI_COMM_WORLD);
    }
    //   printf("Processo %d enviando relogio (%d, %d, %d)\n", resposta.remetente, resposta.clock.p[0], resposta.clock.p[1], resposta.clock.p[2]);

    pthread_mutex_unlock(&mutexSaida);
    pthread_cond_signal(&condFullSaida);
}

void *startThreadEntrada(void *args)
{
    long pid = (long)args;
    while (1 == 1)
    {
        // printf("Processo %d, Thread Receptora...\n", (int) pid);
        getEntrada(pid);
        // sleep(1);
    }
    return NULL;
}

void *startThreadMeio(void *args)
{
    long p = (long)args;

    if (p == 0)
    {
        // Clock clock0 = {{0,0,0}};
        adicionaCanais(1, 0);
        adicionaCanais(2, 0);
        
        initSnapshot(0);
        Event(0, &clockMain);
        
        Send(0, 1, &clockMain);
        Receive(0, &clockMain);
        Send(0, 2, &clockMain);
        Receive(0, &clockMain);
        Send(0, 1, &clockMain);
        Event(0, &clockMain);

        for (int i = 0; i < canaisCount; i++)
        {
            printarListaCanalComunicacao(snapshot.mensagens[canais[i].canal], canais[i].canal, (int)p);
        }

        printf("\n");
    }

    if (p == 1)
    {
        // Clock clock1 = {{0,0,0}};
        Send(1, 0, &clockMain);
        Receive(1, &clockMain);
        Receive(1, &clockMain);
        for (int i = 0; i < canaisCount; i++)
        {
            printarListaCanalComunicacao(snapshot.mensagens[canais[i].canal], canais[i].canal, (int)p);
        }
        printf("\n");
    }

    if (p == 2)
    {
        // Clock clock2 = {{0,0,0}};
        Event(2, &clockMain);
        Send(2, 0, &clockMain);
        Receive(2, &clockMain);
        for (int i = 0; i < canaisCount; i++)
        {
            printarListaCanalComunicacao(snapshot.mensagens[canais[i].canal], canais[i].canal, (int)p);
        }
        printf("\n");
    }
    return NULL;
}

void *startThreadSaida(void *args)
{
    long pid = (long)args;
    while (1 == 1)
    {
        // printf("Processo %d, Thread Emissora...\n", (int) pid);
        submitSaida();
        // sleep(1);
    }
    return NULL;
}

void createThreads(int n)
{
    pthread_t tEntrada; // thread receptora
    pthread_t tMeio;    // thread de relogios vetorias
    pthread_t tSaida;   // thread emissora

    pthread_cond_init(&condEmptyEntrada, NULL);
    pthread_cond_init(&condEmptySaida, NULL);
    pthread_cond_init(&condFullSaida, NULL);
    pthread_cond_init(&condFullEntrada, NULL);
    pthread_cond_init(&condMsgFila, NULL);
    pthread_mutex_init(&mutexEntrada, NULL);
    pthread_mutex_init(&mutexSaida, NULL);
    pthread_mutex_init(&mutexSnapshot, NULL);

    if (pthread_create(&tMeio, NULL, &startThreadMeio, (void *)n) != 0)
    {
        perror("Falha ao criar a thread");
    }

    if (pthread_create(&tEntrada, NULL, &startThreadEntrada, (void *)n) != 0)
    {
        perror("Falha ao criar a thread");
    }

    if (pthread_create(&tSaida, NULL, &startThreadSaida, (void *)n) != 0)
    {
        perror("Falha ao criar a thread");
    }

    if (pthread_join(tMeio, NULL) != 0)
    {
        perror("Falha ao dar join na thread");
    }

    if (pthread_join(tEntrada, NULL) != 0)
    {
        perror("Falha ao dar join na thread");
    }

    if (pthread_join(tSaida, NULL) != 0)
    {
        perror("Falha ao dar join na thread");
    }

    pthread_cond_destroy(&condEmptyEntrada);
    pthread_cond_destroy(&condEmptySaida);
    pthread_cond_destroy(&condFullSaida);
    pthread_cond_destroy(&condFullEntrada);
    pthread_mutex_destroy(&mutexEntrada);
    pthread_mutex_destroy(&mutexSaida);
    pthread_mutex_destroy(&mutexSnapshot);
}

void process0()
{
    int myRank = 0;
    createThreads(myRank);
}

void process1()
{
    int myRank = 1;
    createThreads(myRank);
}

void process2()
{
    int myRank = 2;
    createThreads(myRank);
}

void sendSnapshotMarker(int remetente, int destinatario)
{
    pthread_mutex_lock(&mutexSaida);

    while (qSaidaCount == BUFFER_SIZE)
    { // checa se a fila de saida está cheia
        pthread_cond_wait(&condFullSaida, &mutexSaida);
    }

    // carrega a msg
    Msg *msg = (Msg *)malloc(sizeof(Msg));
    Clock cl = {{-1, -1, -1}};
    msg->clock = cl; // relogio que sinaliza o snapshot
    msg->remetente = remetente;
    msg->destinatario = destinatario; // -1 indica que é para todos os canais de comunicação

    
    
    
    qSaida[qSaidaCount] = *msg;
    qSaidaCount++;
    
    if(destinatario != remetente) {
        printf("[SNAPSHOT]  Processo: %d | Marcador Carregado na Fila\n", remetente);
    }else {
        printf("[SNAPSHOT]  Iniciando...\n");
    }
    
    pthread_mutex_unlock(&mutexSaida);
    pthread_cond_signal(&condEmptySaida);
}

void initSnapshot(int remetente)
{ // remetente = rank
    processoIniciador = remetente;
    sendSnapshotMarker(remetente, remetente); // coloque na fila de mensagens o inicio do snapshot 
    // sendSnapshotMarker(remetente, -1); // depois manda o marcador para os demais
}

int main(int argc, char *argv[])
{
    int my_rank;

    // inicializa o vetor como null
    for (int i = 0; i < 3; i++)
    {
        snapshot.mensagens[i] = NULL;
    }

    MPI_Init(NULL, NULL);
    MPI_Comm_rank(MPI_COMM_WORLD, &my_rank);

    if (my_rank == 0)
        process0();

    else if (my_rank == 1)
        process1();

    else if (my_rank == 2)
        process2();

    MPI_Finalize();

    return 0;
}
