
#include <stdio.h>
#include <pthread.h>
#include <unistd.h>
#include "pipe.h"


#define RUNNING 1
#define STOPPED 0

typedef struct {
    pipe_consumer_t* c;
    int parent_status;
} thread_context_t;

void hexdump_buffer (const char * title, const char * buffer, const int len_buffer)
{
        int i = 0;

        for (i=0; i<len_buffer ;i++)
        {
               printf ("%s>%02x ",title, buffer[i]);
               if (i%16 == 15)
               {
                       printf("\n");
               }
        }
}

void *ProcessPayload ( void *context)
{

    thread_context_t *ctx;
    ctx = context;

    char rx_packet [257];

    size_t a_cnt =0;

    printf("Thread\n");

    while (ctx->parent_status == RUNNING || a_cnt > 0)
    {


	    a_cnt= pipe_pop_eager(ctx->c, rx_packet,  sizeof(rx_packet)/sizeof(*(rx_packet)));

        printf ("Size of rx_packet = %u\n",a_cnt);
	
	    if (a_cnt)
	    {
            // sleep(1);
	        hexdump_buffer ("C",rx_packet,256);
	    }
	    else
	    {
	        printf("empty\n");
	    }
    }
	
    pipe_consumer_free(ctx->c);

    return NULL;
}

void send_data( pipe_producer_t * p){

	char tx_packet [257];

    char fileName_ssdv [20] = "ssdv.bin";
    FILE* file_ssdv = fopen(fileName_ssdv, "rb");

	unsigned int i = 0;

    printf ("Size of tx_packet = %u\n", sizeof(tx_packet)/sizeof(*(tx_packet)));

    while (fread(tx_packet,256,1,file_ssdv) && (i < 10))
    {
        sleep(1);
	    tx_packet[256]='\0';
        hexdump_buffer ("Producer",tx_packet,256);
        
        pipe_push(p, tx_packet,  sizeof(tx_packet)/sizeof(*(tx_packet)));
    
	    i++;
    }

	fclose(file_ssdv);

}

int main (void)
{

    pipe_t* pipe = pipe_new(sizeof(char), 257000);
    pipe_producer_t* p = pipe_producer_new(pipe);
    pipe_consumer_t* c = pipe_consumer_new(pipe);
    pipe_free(pipe);

    pthread_t process_thread;

    thread_context_t ctx;

    ctx.c = c;
    ctx.parent_status = RUNNING;
    
    int rc = 0;

    rc = pthread_create(&process_thread, NULL, ProcessPayload, (void *)&ctx);
    if (rc){
       printf("ERROR; return code from pthread_create() is %d\n", rc);
       return(-1);
    }
    send_data(p);

    pipe_producer_free(p);

    ctx.parent_status = STOPPED;

    pthread_join(process_thread, NULL);

    return (0);
}

