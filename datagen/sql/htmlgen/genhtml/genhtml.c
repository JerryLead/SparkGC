/* 

Usage: genhtml [-z number -ns number -s number -f number -r number]
	-z number: zipfian parameter (default .05)
	-ns number: number of sites (default 100)
	-s number: this site number (default 0)
	-f number: files per site (default 100000)
	-r number: random number seed (default 0)

   Generate a collection of random html documents with links in them.
   
   Designed to be used in a multi-site setting, where each site is
   responsible for a collection of documents.

   Each HTML document is given a random name (e.g., #abdhgfas.html),
   where "#" is the site #.  Documents consist of words, and links to
   other documents (which may be on other sites.)  E.g.:

   > more docs/0/1emkl.html

   <html><head></head><body> 
   catalogical borosalicylate decarnated anesthesiology 
   <a href="http://4aijeeggblklljilgihlaleemlglfkdbk.html">link</a>
   Dardani autochemical cubmaster axhead aspergillosis circumcise
   batholite <a href="http://2dcjcijemldgddhkib
   gikbllljhkkdehghejdbb.html">link</a> bandog Auricula 
   <a href="http://4mdfmgcbffkbblbffedgbgkdeajamjgkjjdidfjik.html">link</a>
   chlorometer anammo nid appulsively chronaxy Calotermes citraconic
   cavalla Brittany archaizer bipedism cactaceous decarburize Chronos
   cobbling <a href="http://1clkjllabhcdidkflijffemdbgiikb.html">link</a> dassie
   <a href="http://0iagldillbcdhdakhiahkjimljbcjbaikbciekgcmjfgm.html">link</a>
   cloakless bill calcariform <a href="http://0khdaghhaiaefmmbhgfkfjll.html">link</a> 
   crystallology dareall co axial arite antiaquatic adjection dapicho brob
   commerciality blastogranitic dandiacally cragsman
   ...

   The program generates the documents for a given siteid (which is
   specified on the command line.)

   It is important that all sites be run with the same parameters
   (including random number seed) so that all sites have the same url
   names in them.

   Files will be output in 1000 file batches subdirectories under the
   docs directory, named: docs/0/, docs/1000/, docs/2000/, ... .  This
   is done because the performance of most file systems degrades
   dramatically as directories get very large.  Files are named
   according to their url.

   Program will create the docs directory, but you may wish to remove
   old files from it.

   Author: Sam Madden
   Revision History: 
   
   .1 (this version) : Initial release
*/

#include <stdio.h>
#include <stdlib.h>
#include <math.h>
#include <sys/time.h>
#include <strings.h>

typedef enum {
  UNIFORM,
  ZIPFIAN
} e;

//configurable via command line
double zipf_param = .5; //weighting parameter for zipfian on file popularity
int filespersite = 100000;
int numsites = 100;
int siteno = 0;
long randseed = -1;

//constants, for now
int distribution = ZIPFIAN;
double link_freq = .05;  //probability of links vs. plain words
int maxurllen = 100;
int minurllen = 10;
double len_u =  10000; //average file length
double len_var = 1000;  //average file variance

//global state
char **urls; //create by gen_urls
char *dictf = "/usr/share/dict/words";
int maxwords = 100000;
int numwords = 0; //number of words in words
char **words;  //words array -- created by load_words
int *zipf; //zipfian -- create by load_zipf
int num_bins; //size of zipf


char *usage = "\nUsage: genhtml [-z number -ns number -s number -f number -r number]\n\t-z number: zipfian parameter (default .5)\n\t-ns number: number of sites (default 100)\n\t-s number: this site number (default 0)\n\t-f number: files per site (default 100000)\n\t-r number: random number seed (default 0)\n";

/* Parse the input arguments, write values into global variables */
void parse_args(int argc,char **argv) {
  int curarg = 1;
  while (curarg < argc) {
    if (strcmp(argv[curarg], "-z") == 0 ) {
      if (++curarg < argc) {
	sscanf(argv[curarg],"%lf",&zipf_param);
	printf ("zipfian = %f\n",zipf_param);
      } else {
	printf ("Expected number after -z\n");
	printf ("%s",usage);
	exit(1);
      }

    } else if (strcmp(argv[curarg],"-ns") ==0 ) {
      if (++curarg < argc) {
	sscanf(argv[curarg],"%d",&numsites);
      } else {
	printf ("Expected number after -ns\n");
	printf ("%s",usage);
	exit(1);
      }
    } else if (strcmp(argv[curarg],"-s") ==0 ) {
      if (++curarg < argc) {
	sscanf(argv[curarg],"%d",&siteno);
      } else {
	printf ("Expected number after -s\n");
	printf ("%s",usage);
	exit(1);
      }

    } else if (strcmp(argv[curarg],"-f") ==0 ) {
      if (++curarg < argc) {
	sscanf(argv[curarg],"%d",&filespersite);
      } else {
	printf ("Expected number after -f\n");
	printf ("%s",usage);
	exit(1);
      }
    } else if (strcmp(argv[curarg],"-r") ==0 ) {
      if (++curarg < argc) {
	sscanf(argv[curarg],"%d",&randseed);
      } else {
	printf ("Expected number after -r\n");
	printf ("%s",usage);
	exit(1);
      }
    } else {
      printf ("Unknown argument %s\n", argv[curarg]);
      printf ("%s", usage);
      exit(1);
    }
    curarg++;
  }
}



#define myitoa(c) (c) + 'a'

#define frand() ((float)random() / ((double)pow(2,31)-1))

/* Populate list of random URLs for all sites.
   Use A LOT of RAM 
*/

void gen_urls() {
  int numurls = numsites * filespersite;
  int i,j,k;

  urls = (char **)malloc(sizeof(char *) * numurls);
  for (i = 0; i < numsites; i++) {
    for (j = 0; j < filespersite; j++) {
      int len = (int)((frand() * (maxurllen - minurllen)) + minurllen);
      //printf ("len = %d\n", len);
      char *str = (char *)malloc((sizeof(char) * len)+17);
      urls[i * filespersite + j] = str;
      sprintf(str, "http://%d", i);
      for ( k = strlen(str); k < len; k++) {
	str[k] = myitoa(26 * frand());
      }
      str[len] =0;
      sprintf(str + strlen(str), ".html");
      //printf ("%s\n",str);
    }

  }
}

/* Load list of words from dictionar into words array.  numwords contains
 the number of words after execution. */

void load_words() {
  FILE *f=fopen(dictf, "r");
  words = (char **)malloc(sizeof(char *) * maxwords);
  char word[100];
  char *neww;

  if (f != NULL) {
    while ((fscanf(f, "%s\n", word)) != EOF) {
      neww = (char *)malloc(strlen(word) * sizeof(char) + 1);
      strcpy (neww, word);
      words[numwords++] = neww;
      if (numwords == maxwords) break;
    }
    fclose(f);
  } else {
    printf("Couldn't open %s\n", dictf);
    exit(1);
  }
}

/* Generate a zipfian distribution in the zipf array of length num_bins.
   The ith element of this array contains the id of the url
   at sample i/num_bins in the zipfian distribution with parameter
   zipf_param
*/ 
void load_zipf() {
  int i,j;
  int numurls = numsites * filespersite;
  double sum = 0.0;
  int min_bucket = 0;
  int max_bucket;
  double residual = 0;


  num_bins = numsites * filespersite * 10;
 
  zipf = (int *)malloc(sizeof(int) * num_bins);

  for (i = 1; i <= numurls; i++) {
    double val = 1.0/pow((double)i, zipf_param);
    sum = sum + val;

  }

  for (i = 0; i < numurls; i++) {
    double link_prob =  (1.0 / pow((double)(i+1), zipf_param)) / sum;
    max_bucket = min_bucket + num_bins * (link_prob + residual);
    for (j = min_bucket; j < max_bucket; j++) {
      zipf[j] = i;
    }
    residual += link_prob - ((double)(max_bucket - min_bucket) / (double)num_bins);
    if (residual < 0) residual = 0;
    min_bucket = max_bucket;
  }

  //  for ( i = 0; i < num_bins; i++) {
  //printf ("%d\n", zipf[i]);
  //}

		
}

//return a random word
#define get_word() words[(int)((numwords-1) * frand())]

//from knuth -- return a sample from a (0,1) normal distributon
double random_normal() {
  double u1, u2, v1, v2;
  double s= 2;
  while (s >= 1) {
    u1 = frand();
    u2 = frand();
    v1 = 2.0 * u1-1.0;
    v2 = 2.0 * u2-1.0;
    s = pow(v1,2) + pow(v2,2);
  }
  double x1 = v1 * sqrt((-2.0 * log(s))/s);
  return x1;
}

//return the length in bytes of a document, where length is
// sampled from a normal distribution with mean len_u and
// variance len_var
int doc_len() {
  return (int)((double)len_u + random_normal() * len_var);
}

//return a random URL from the url list
char* get_url() {
  if (distribution == ZIPFIAN) {
    return urls[zipf[(int)(num_bins * frand())]];
  } else { //UNIFORM
    return urls[(int)(frand() * (numsites * filespersite))];
  }
}

//generate one html file in file name fname
void gen_file(char *fname, int i) {
  char progress[200];

  int len = doc_len();
    if ( 0 && i >= 20800 )
      {
	sprintf(progress, "echo 'doc len %d' >> /scratch/report.txt",(len));
        system(progress);
      }
    int linesofar = 0, total_so_far = 0;
  FILE *f = fopen(fname, "w");
  int link = 0;

  if (f != NULL) {
    fprintf(f, "<html><head></head><body>\n");
    while (1) {
      char *nextw;

    if ( 0 && i >= 20800 )
      {
	sprintf(progress, "echo 'lines so far len %d TOTAL %d' >> /scratch/report.txt",(linesofar), total_so_far);
        system(progress);
      }
      
      if (frand() < link_freq) {
	if ( 0 && i >= 20800 )
	  {
	    sprintf(progress, "echo '----------get URL ' >> /scratch/report.txt");
	    system(progress);
	  }


	nextw = get_url();
	link = 1;
      } else {
	if ( 0 && i >= 20800 )
          {
            sprintf(progress, "echo '----------get WORD ' >> /scratch/report.txt");
            system(progress);
          }

	nextw = get_word();
	link =0;
      }
      if (link) {
	fprintf(f,"<a href=\"%s\">link</a> ",nextw);
      } else {
	fprintf(f,"%s ",nextw);
      }
      linesofar = linesofar + strlen(nextw);
      if (linesofar > 80 ) {
	fprintf(f, "\n");
	total_so_far += linesofar + 1;
	if (total_so_far > len)
	  break;
	linesofar = 0;
      }
    }
    fprintf(f, "\n</body></html>");

  } else {
    printf ("Couldn't open %s\n", fname);
    exit(1);
  }
  fclose(f);
}

//generate the html documents for this
//site, in docs/0/, docs/1000/, ... directories
void gen_docs() {
  int start_doc = siteno * filespersite;
  int end_doc = start_doc + filespersite;
  int i;
  char docname[200];
  char subdir[10], cmd[30];
  char progress[200];

  sprintf(subdir, "0");
  system ("mkdir -p docs/0/");
  for (i = start_doc; i < end_doc; i++) {
    if (0 && i%100==0 ) //|| (i-start_doc > 20000))
      {
	sprintf(progress, "echo 'writing doc %d' >> /scratch/report.txt",(i-start_doc));
	system(progress); 
      }
       if ((i % 5000) == 0) {
      printf("Writing doc %d\n", i - start_doc);
      sprintf(subdir, "%d", i);
      sprintf(cmd, "mkdir -p docs/%s/", subdir);
      system(cmd);
      }
    
    sprintf(docname,"docs/%s/%s", subdir,urls[i]+7);
    if ( 0 && i -start_doc >= 20800 )
      {
	sprintf(progress, "echo 'writing doc %s' >> /scratch/report.txt",(char*)docname);
	system(progress); 
      }
      gen_file(docname, (i-start_doc));
    if ( 0 && i -start_doc >= 20800 )
      {
	sprintf(progress, "echo 'DONE %s' >> /scratch/report.txt",(char*)docname);
	system(progress); 
      }
  }
}

int main(int argc, char **argv)
{
  parse_args(argc,argv);

  if (randseed == -1){
    randseed = 0; //important that all sites use the same rand seed
  }
  srandom(randseed);

  printf ("generating URLS\n");
  gen_urls();
  printf ("loading words\n");
  load_words();
  printf ("building zipfian\n");
  load_zipf();
  printf ("writing docs for site %d\n", siteno);
  gen_docs();

  return 0;
}
