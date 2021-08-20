#include "common.h"
#include "BoundedBuffer.h"
#include "Histogram.h"
#include "common.h"
#include "HistogramCollection.h"
#include "NRC.h"
using namespace std;


struct p_arg
{
    int n;
    int p;
    int w;
    int b;
    NRC* chan;
    string file_name;
    bool file_run = false;
    BoundedBuffer * buff;
    p_arg(int n,int p, BoundedBuffer * buff,bool file_run,string file_name):n(n),p(p),buff(buff),file_run(file_run),file_name(file_name){};
    p_arg(int n,int p, BoundedBuffer * buff,bool file_run,string file_name,NRC* chan):n(n),p(p),buff(buff),file_run(file_run),file_name(file_name),chan(chan){};
};
struct w_arg
{
    mutex mtx;
    ofstream out;
    BoundedBuffer * buff;
    NRC* chan;
    HistogramCollection * hc;
    bool file_run;
    string file_name;
    w_arg(BoundedBuffer * buff, NRC* chan,HistogramCollection * hc,bool file_run,string file_name, ofstream& out):buff(buff),chan(chan),hc(hc),file_run(file_run),file_name(file_name){};
    w_arg(BoundedBuffer * buff, NRC* chan,HistogramCollection * hc,bool file_run,string file_name):buff(buff),chan(chan),hc(hc),file_run(file_run),file_name(file_name){};
};


void * patient_function(void* arg)
{
    /* What will the patient threads do? */
        
    //cout << "made it this far1" << endl;
    p_arg * dat_in = (p_arg*) arg;
    if(dat_in->file_run == false)
    {
        for (double i = 0; i < dat_in->n*.004; i=i+0.004)
        {
            if (i>59.996)
            {
                break;
            }
            //cout << dat_in->p << " " << i << endl;
            datamsg * data = new datamsg(dat_in->p,i,1);
            // cout<< data->person<< " intial_person"<<endl;
            // cout<< data-> seconds<< " intial_seconds"<<endl;
            // cout<< data-> ecgno << " intial_ECG"<<endl;
            char * temp = (char*)data;
            vector<char> temp_vec (temp,temp+sizeof(datamsg));
            dat_in->buff->push(temp_vec);
                
        }
    }
    else if (dat_in->file_run == true)
    {
        // string out_loc = "received/";
        // string out_file = "y";
        // out_file += dat_in->file_name;
        // out_loc += out_file;
        //fstream out_stream; 
        //out_stream.open(out_loc, fstream::out | fstream::binary); // worker
        filemsg D_File(0,0);
        int size =  sizeof(dat_in->file_name) + sizeof(D_File);
        char* buffer = new char[size + 1];
        memcpy(buffer,&D_File, sizeof(filemsg));
        strcpy(buffer+sizeof(filemsg), dat_in->file_name.c_str());
        dat_in->chan->cwrite(buffer, size+1);
        //cout << "just before cwrite"<< endl;
//        filemsg msg = filemsg(0,0);
//        int size2 = sizeof(msg)+strlen(dat_in->file_name.c_str()+1);
//        char* array_size = new char[size2];
//        memcpy(array_size,&msg,sizeof(filemsg));
//        char *next = array_size + sizeof(filemsg);
//        strcpy(next, dat_in->file_name.c_str());
//        dat_in->chan->cwrite(array_size, size2);
        //cout << "cwrite ran1" << endl;
        char* out_size = dat_in->chan->cread();
        memcpy(buffer, &D_File, sizeof(filemsg));
        size = *(int*) out_size;
        //cout<<"this works 1"<<endl;
        MESSAGE_TYPE m = QUIT_MSG;
        dat_in->chan->cwrite ((char*)&m, sizeof(QUIT_MSG));
        delete dat_in->chan;
        
        int start_pt = 0;

        while(size != 0)
        {
            char* fin_buff = new char[sizeof(filemsg) + sizeof(dat_in->file_name)];

            if(size > MAX_MESSAGE) // if greater than 256 bytes
            {
                filemsg file_msg = filemsg(start_pt, MAX_MESSAGE);
                memcpy(buffer, &file_msg, sizeof(file_msg));
                //chan->cwrite(fin_buff, sizeof(file_msg));               // cwrite cread here
                vector<char> fin_buff_vec (buffer,buffer+(sizeof(filemsg) + sizeof(dat_in->file_name)));
                dat_in->buff->push(fin_buff_vec);
                //char* fin_out = chan->cread();
                start_pt += MAX_MESSAGE;
                size -= MAX_MESSAGE;
                //out_stream.write(fin_out,MAX_MESSAGE);
            }
            else // smaller than 256 bytes
            {
                filemsg file_msg = filemsg(start_pt, size);
                memcpy(buffer, &file_msg, sizeof(file_msg));
                //chan->cwrite(fin_buff, sizeof(file_msg));
                vector<char> fin_buff_vec (buffer,buffer+(sizeof(filemsg) + sizeof(dat_in->file_name)));
                dat_in->buff->push(fin_buff_vec);
                //char* fin_out = chan->cread();
                //out_stream.write(fin_out,size);
                size = 0;
            }
        }
        //out_stream.close();
    }
    
    return nullptr;
}

void *worker_function(void* arg)
{
    w_arg * dat_out = (w_arg*) arg;
    while (true)
    {
        //w_arg * dat_out = (w_arg*) arg;
        vector<char> temp_vec = dat_out->buff->pop();
        char * temp = reinterpret_cast<char*>(temp_vec.data());
        datamsg * data = (datamsg*) temp;
        //cout<< data->person<< " person"<<endl;
        //cout<< data-> seconds<< " seconds"<<endl;
        //cout<< data-> ecgno << " ECG"<<endl;
        if (data->mtype == QUIT_MSG)
        {
            //cout<<"ran "<<endl;
            //MESSAGE_TYPE m = QUIT_MSG;
            dat_out->chan->cwrite (temp, sizeof(datamsg));
            delete dat_out->chan;
            //cout<<"broke here"<<endl;
            return nullptr;
        }
        else if(data->mtype == DATA_MSG)
        {
            dat_out->chan->cwrite(temp, sizeof(datamsg));
            char* msg = dat_out->chan->cread();
            dat_out->hc->update(*(double *) msg,data->person);
            //cout << "The ECG data for this patient: "<< *(double *) msg << endl;
        }
        else
        {
            // dat_out->chan->cwrite((char*)data, temp_vec.size());
            // char* msg = dat_out->chan->cread();
            //for (int i = 0; i < 256; i++)
            // {
            //     cout << msg[i];
            // }
            filemsg * file_msg = (filemsg*) temp;
            // char * filename = temp + sizeof(filemsg);
            // cout << filename << endl;
            dat_out->chan->cwrite(temp, temp_vec.size());
            char* msg = dat_out->chan->cread();
            dat_out->mtx.lock();
            dat_out->out.open(dat_out->file_name, ios_base::in | ios_base::out | ios_base::binary);
            dat_out->out.seekp(file_msg->offset);
            dat_out->out.write(msg, file_msg->length);
            dat_out->out.close();
            dat_out->mtx.unlock();
            
        }
        
        
        
    }
}


int main(int argc, char *argv[])
{
    int option;
    bool file_run = false;
    string file;
    string host_name;
    string port;
    int n = 15000;    //default number of requests per "patient"
    int p = 15;     // number of patients [1,15]
    int w = 500;    //default number of worker threads
    int b = 1; 	// default capacity of the request buffer, you should change this default
	int m = MAX_MESSAGE; 	// default capacity of the file buffer
    srand(time_t(NULL));

   while((option = getopt(argc, argv, "n:p:w:b:f:h:r:"))!=-1)
   {
    switch (option)
    {
      case 'n':
      n = atoi(optarg);
      //cout << pat << endl;
      break;
  
      case 'p':
      p = atoi(optarg);
      break;

      case 'w':
      w = atoi(optarg);
      break;
  
      case 'b':
      b = atoi(optarg);
      //cout << file << endl;
      break;
      
      case 'f':
      p = 1;
      //cout<<"broke here"<< endl;
      file = optarg;
      //cout<<"broke here confirm"<< endl;
      file_run = true;
      //cout<<"broke here"<< endl;
      break;
    
      case 'h':
      host_name = optarg;
      break;
            
      case 'r':
      port = optarg;
      break;
            
    }
  }
    
    //cout << "made channel request"<< endl;
    BoundedBuffer request_buffer(b);
	HistogramCollection hc;
    //cout << "connection made";
	
	
    struct timeval start, end;
    gettimeofday (&start, 0);

    /* Start all threads here */
    
    // patient threads
    //cout << "made it pat threads"<< endl;
    vector<thread> pat_threads;
    vector<thread> work_threads;
    if (file_run == true)
    {   
        fstream out_stream;
        //string out_loc = "received/";
        //string out_file = "y";
        //out_file += file;
        string out_loc = file;
        out_stream.open(out_loc, ios::out | ios::trunc);
        out_stream.close();
        for (int i = 0; i < p; i++)
        {
            NRC* new_chan = new NRC (host_name, port);
            p_arg * pat = new p_arg(n,i+1,&request_buffer,file_run,file, new_chan);
            pat_threads.push_back(thread(patient_function,pat));
        }
        
        for (int i = 0; i < w; i++)
        {
            NRC* new_chan = new NRC (host_name, port);
            w_arg * work = new w_arg(&request_buffer,new_chan,&hc,file_run,file);
            work_threads.push_back(thread(worker_function,work));
        }
        
    }
    else
    {
        //vector<thread> pat_threads;
        for (int i = 0; i < p; i++)
        {
            Histogram * temp = new Histogram(10,-2,2);
            hc.add(temp); 
            p_arg * pat = new p_arg(n,i+1,&request_buffer,file_run,file);
            pat_threads.push_back(thread(patient_function,pat));
        }
        
        for (int i = 0; i < w; i++)
        {
            
            NRC* new_chan = new NRC (host_name, port);
            //work->chan = new_chan;
            w_arg * work = new w_arg(&request_buffer,new_chan,&hc,file_run,file);
            work_threads.push_back(thread(worker_function,work));
        }
        
    }
    
    /* Join all threads here */

    //ending patient threads
    for (int i = 0; i < p; i++)
    {
        pat_threads[i].join();
    }
    //cout <<"pat_threads joined"<<endl;

    //char* quit_th = (char*)(new quit_msg);
    //vector<char>quit(quit_th, quit_th + sizeof(quit_msg));
    for (int i = 0; i < w; i++)
    {
        MESSAGE_TYPE q = QUIT_MSG;
        char* temp = (char*)&q;
        vector<char> quit(temp,temp+(sizeof(QUIT_MSG)));
        request_buffer.push(quit);
    }
    
    //ending worker threads
    for (int i = 0; i < w; i++)
    {
       work_threads[i].join();
    }
    
    gettimeofday (&end, 0);
	hc.print ();
    int secs = (end.tv_sec * 1e6 + end.tv_usec - start.tv_sec * 1e6 - start.tv_usec)/(int) 1e6;
    int usecs = (int)(end.tv_sec * 1e6 + end.tv_usec - start.tv_sec * 1e6 - start.tv_usec)%((int) 1e6);
    cout << "Took " << secs << " seconds and " << usecs << " micor seconds" << endl;
//
//    MESSAGE_TYPE q = QUIT_MSG;
//    chan->cwrite ((char *) &q, sizeof (MESSAGE_TYPE));
    cout << "All Done!!!" << endl;
}
