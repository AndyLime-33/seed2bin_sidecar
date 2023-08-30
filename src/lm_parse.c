/***************************************************************************
 * A program for libmseed parsing tests.
 *
 * This file is part of the miniSEED Library.
 *
 * Copyright (c) 2020 Chad Trabant, IRIS Data Management Center
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 ***************************************************************************/

#include <errno.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>

#include "libmseed.h" //vær varsom med include path til linkede filer
						//path er : C:\Users\anders sikker\Documents\projekter\libmseed-main
#include "mseedformat.h"
#define PACKAGE "lm_parse"
#define VERSION "[libmseed " LIBMSEED_VERSION " " PACKAGE " ]"

static flag verbose    = 0;
static flag ppackets   = 0;
static flag basicsum   = 0;
static flag tracegap   = 0;
static int printraw    = 0;
static int printdata   = 0;
static int reclen      = -1;
static char *inputfile = NULL;

static int parameter_proc (int argcount, char **argvec);
static void print_stderr (const char *message);
static void usage (void);
char blk2000[1000*1000];
unsigned blk2000pos;

int ms_parse_raw2000 (char *record, int maxreclen, int8_t details, int8_t swapflag);
/* Binary I/O for Windows platforms */
#ifdef LMP_WIN
  #include <fcntl.h>
  unsigned int _CRT_fmode = _O_BINARY;
#endif
#undef stderr
#define stderr stdout
int
main (int argc, char **argv)
{
	setvbuf(stdout, NULL, _IONBF, 0);//printf unbuffer aht debug
	setvbuf(stderr, NULL, _IONBF, 0);
  MS3TraceList *mstl = 0;
  MS3Record *msr = 0;
  uint32_t flags = 0;
printf("getting started\n");
  int64_t totalrecs = 0;
  int64_t totalsamps = 0;
  int retcode;
  /* Redirect libmseed logging facility to stderr for consistency */
  ms_loginit (print_stderr, NULL, print_stderr, NULL);

  /* Process command line arguments */
  if (parameter_proc (argc, argv) < 0)
    return -1;

  /* Validate CRC and unpack data samples */
  flags |= MSF_VALIDATECRC;

  /* Parse byte range from file/URL path name if present */
  flags |= MSF_PNAMERANGE;

  if (printdata)
    flags |= MSF_UNPACKDATA;
  tracegap=1;
  if (tracegap)
    mstl = mstl3_init (NULL);

  /* Loop over the input file */
  LARGE_INTEGER StartingTime, EndingTime, ElapsedMicroseconds;
  LARGE_INTEGER Frequency;

  QueryPerformanceFrequency(&Frequency);
  QueryPerformanceCounter(&StartingTime);


  while ((retcode = ms3_readmsr (&msr, inputfile, NULL, NULL, flags|MSF_UNPACKDATA,
                                 verbose)) == MS_NOERROR)
  {
	  totalrecs++;
    totalsamps += msr->samplecnt;

    if (tracegap)
    {
    	//changed - ignoring zero length records, most likely blockette 2000
      if(msr->samplecnt) mstl3_addmsr (mstl, msr, 0, 1, flags, NULL);
      else {
    	  ms_parse_raw2000 (msr->record, msr->reclen, 0, -1);
      }
    }
    else
    {
      if ( printraw )
      {
        if (msr->formatversion == 3)
          ms_parse_raw3 (msr->record, msr->reclen, ppackets);
        else
          ms_parse_raw2 (msr->record, msr->reclen, ppackets, -1);
      }
      else
      {
        msr3_print (msr, ppackets);
      }

      if (printdata && msr->numsamples > 0)
      {
        int line, col, cnt, samplesize;
        int lines = (msr->numsamples / 6) + 1;
        void *sptr;

        if ((samplesize = ms_samplesize (msr->sampletype)) == 0)
        {
          ms_log (2, "Unrecognized sample type: '%c'\n", msr->sampletype);
        }
        if (msr->sampletype == 'a')
        {
          char *ascii = (char *)msr->datasamples;
          int length  = msr->numsamples;

          ms_log (0, "ASCII Data:\n");

          /* Print maximum log message segments */
          while (length > (MAX_LOG_MSG_LENGTH - 1))
          {
            ms_log (0, "%.*s", (MAX_LOG_MSG_LENGTH - 1), ascii);
            ascii += MAX_LOG_MSG_LENGTH - 1;
            length -= MAX_LOG_MSG_LENGTH - 1;
          }

          /* Print any remaining ASCII and add a newline */
          if (length > 0)
          {
            ms_log (0, "%.*s\n", length, ascii);
          }
          else
          {
            ms_log (0, "\n");
          }
        }
        else
          for (cnt = 0, line = 0; line < lines; line++)
          {
            for (col = 0; col < 6; col++)
            {
              if (cnt < msr->numsamples)
              {
                sptr = (char *)msr->datasamples + (cnt * samplesize);

                if (msr->sampletype == 'i')
                  ms_log (0, "%10d  ", *(int32_t *)sptr);

                else if (msr->sampletype == 'f')
                  ms_log (0, "%10.8g  ", *(float *)sptr);

                else if (msr->sampletype == 'd')
                  ms_log (0, "%10.10g  ", *(double *)sptr);

                cnt++;
              }
            }
            ms_log (0, "\n");

            /* If only printing the first 6 samples break out here */
            if (printdata == 1)
              break;
          }
      }
    }
  }

  if (retcode != MS_ENDOFFILE)
    ms_log (2, "Cannot read %s: %s\n", inputfile, ms_errorstr (retcode));

  if (tracegap)
    mstl3_printtracelist (mstl, SEEDORDINAL, 1, 1);
	char sidecarfilebuf[1000];
	char *sidecarfile=NULL;
	char tracefilebuf[1000];
	char *tracefile=NULL;
	char blk2000filebuf[1000];
	char *blk2000file=NULL;

  if(inputfile) {
  	char *dot=strrchr(inputfile,'.');
  	if(dot) {
  		unsigned dotlen=1+dot-inputfile;
  		snprintf(sidecarfilebuf,sizeof(sidecarfilebuf),"%.*ssidecar",dotlen,inputfile);
  		snprintf(tracefilebuf,sizeof(tracefilebuf),"%.*strace",dotlen,inputfile);
  		tracefile=tracefilebuf;
  		snprintf(blk2000filebuf,sizeof(blk2000filebuf),"%.*sblk2000",dotlen,inputfile);
  		blk2000file=blk2000filebuf;
  		sidecarfile=sidecarfilebuf;
  	}

  char rsp[1000];
  unsigned pos=0;
  unsigned maxlen= sizeof(rsp);
  pos+=snprintf(&rsp[pos],maxlen-pos,"[");
  MS3TraceID *id = mstl->traces;
  while(id) {

	  MS3TraceSeg *seg = id->first;
	  if(seg->next) {
		  printf("ERROR GAPS\n");
		  return -1;
	  }
	  char network[64];
	  char station[64];
	  char location[64];
	  char channel[64];
	  if (ms_sid2nslc (id->sid, network, station, location, channel))
	  {
	    ms_log (2, "%s: Cannot parse SEED identifier codes from full identifier\n", msr->sid);
	    return -1;
	  }
	  static char channel_id[64];
	  snprintf(channel_id,sizeof(channel_id),"%s:%s:%s:%s",network,station,location,channel);

	  pos+=snprintf(&rsp[pos],maxlen-pos,"{\"channel\":\"%s\",",channel_id);
	  pos+=snprintf(&rsp[pos],maxlen-pos,"\"samples\":%d,",(int)seg->numsamples);
	  pos+=snprintf(&rsp[pos],maxlen-pos,"\"sr\":%d,",(int)seg->samprate);
	  char stime[30];

	  ms_nstime2timestr (seg->starttime, stime, SEEDORDINAL, NANO_MICRO);
	  int year,yday,hour,minutes,sec,usec;
	  sscanf(stime,"%4d,%03d,%02d:%02d:%02d.%06d",&year,&yday,&hour,&minutes,&sec,&usec);
	  pos+=snprintf(&rsp[pos],maxlen-pos,"\"starttime\":[%d,%d,%d,%d,%d.%d]",year,yday,hour,minutes,sec,usec);
	  pos+=snprintf(&rsp[pos],maxlen-pos,"}");
//printf("channel id = %s\n",channel_id);
	  //
	  id = id->next;
	  if(id) pos+=snprintf(&rsp[pos],maxlen-pos,",");
  }
  pos+=snprintf(&rsp[pos],maxlen-pos,"]");
if(inputfile) {
		FILE *f=fopen(sidecarfile,"wb");
		if(f) {
			fwrite(rsp,pos,1,f);
			fclose(f);
		}
		f=fopen(tracefile,"wb");
		  id = mstl->traces;
		  while(id) {
			  typedef int adcbuf[16];
			  adcbuf * buf = id->first->datasamples;
			  fwrite(id->first->datasamples,id->first->numsamples,sizeof(int),f);
		  id = id->next;
		  }
		fclose(f);
		if(blk2000pos) {
			f=fopen(blk2000file,"wb");
			if(f) {
				fwrite(blk2000,blk2000pos,1,f);
				fclose(f);
			}

		}
}
}


  QueryPerformanceCounter(&EndingTime);
  ElapsedMicroseconds.QuadPart = EndingTime.QuadPart - StartingTime.QuadPart;


  //
  // We now have the elapsed number of ticks, along with the
  // number of ticks-per-second. We use these values
  // to convert to the number of elapsed microseconds.
  // To guard against loss-of-precision, we convert
  // to microseconds *before* dividing by ticks-per-second.
  //

  ElapsedMicroseconds.QuadPart *= 1000000;
  ElapsedMicroseconds.QuadPart /= Frequency.QuadPart;

  /* Make sure everything is cleaned up */
  ms3_readmsr (&msr, NULL, NULL, NULL, flags, 0);

  if (mstl)
    mstl3_free (&mstl, 0);

  if (basicsum)
    ms_log (1, "Records: %" PRId64 ", Samples: %" PRId64 "\n",
            totalrecs, totalsamps);

  return 0;
} /* End of main() */

/***************************************************************************
 * parameter_proc():
 * Process the command line arguments.
 *
 * Returns 0 on success, and -1 on failure
 ***************************************************************************/
static int
parameter_proc (int argcount, char **argvec)
{
  int optind;

  /* Process all command line arguments */
  for (optind = 1; optind < argcount; optind++)
  {
    if (strcmp (argvec[optind], "-V") == 0)
    {
      ms_log (1, "%s version: %s\n", PACKAGE, VERSION);
      exit (0);
    }
    else if (strcmp (argvec[optind], "-h") == 0)
    {
      usage ();
      exit (0);
    }
    else if (strncmp (argvec[optind], "-v", 2) == 0)
    {
      verbose += strspn (&argvec[optind][1], "v");
    }
    else if (strncmp (argvec[optind], "-p", 2) == 0)
    {
      ppackets += strspn (&argvec[optind][1], "p");
    }
    else if (strncmp (argvec[optind], "-z", 2) == 0)
    {
      printraw = 1;
    }
    else if (strncmp (argvec[optind], "-d", 2) == 0)
    {
      printdata = 1;
    }
    else if (strncmp (argvec[optind], "-D", 2) == 0)
    {
      printdata = 2;
    }
    else if (strncmp (argvec[optind], "-tg", 3) == 0)
    {
      tracegap = 1;
    }
    else if (strcmp (argvec[optind], "-s") == 0)
    {
      basicsum = 1;
    }
    else if (strcmp (argvec[optind], "-r") == 0)
    {
      reclen = atoi (argvec[++optind]);
    }
    else if (strncmp (argvec[optind], "-", 1) == 0 &&
             strlen (argvec[optind]) > 1)
    {
      ms_log (2, "Unknown option: %s\n", argvec[optind]);
      exit (1);
    }
    else if (inputfile == NULL)
    {
      inputfile = argvec[optind];
    }
    else
    {
      ms_log (2, "Unknown option: %s\n", argvec[optind]);
      exit (1);
    }
  }

  /* Make sure an inputfile was specified */
  if (!inputfile)
  {
    ms_log (2, "No input file was specified\n\n");
    ms_log (1, "%s version %s\n\n", PACKAGE, VERSION);
    ms_log (1, "Try %s -h for usage\n", PACKAGE);
    exit (1);
  }

  /* Add program name and version to User-Agent for URL-based requests */
  if (libmseed_url_support() && ms3_url_useragent(PACKAGE, VERSION))
    return -1;

  /* Report the program version */
  if (verbose)
    ms_log (1, "%s version: %s\n", PACKAGE, VERSION);

  return 0;
} /* End of parameter_proc() */

/***************************************************************************
 * print_stderr():
 * Print messsage to stderr.
 ***************************************************************************/
static void
print_stderr (const char *message)
{
  fprintf (stderr, "%s", message);
} /* End of print_stderr() */

/***************************************************************************
 * usage():
 * Print the usage message and exit.
 ***************************************************************************/
static void
usage (void)
{
  fprintf (stderr, "%s version: %s\n\n", PACKAGE, VERSION);
  fprintf (stderr, "Usage: %s [options] file\n\n", PACKAGE);
  fprintf (stderr,
           " ## Options ##\n"
           " -V             Report program version\n"
           " -h             Show this usage message\n"
           " -v             Be more verbose, multiple flags can be used\n"
           " -p             Print details of header, multiple flags can be used\n"
           " -z             Print raw details of header\n"
           " -d             Print first 6 sample values\n"
           " -D             Print all sample values\n"
           " -tg            Print trace listing with gap information\n"
           " -s             Print a basic summary after processing a file\n"
           " -r bytes       Specify record length in bytes, required if no Blockette 1000\n"
           "\n"
           " file           File of miniSEED records\n"
           "\n");
} /* End of usage() */

/**********************************************************************/ /**
 * @brief Parse and verify a miniSEED 2.x record header
 *
 * Parsing is done at the lowest level, printing error messages for
 * invalid header values and optionally print raw header values.
 *
 * The buffer \a record is assumed to be a miniSEED record.  Not every
 * possible test is performed, common errors and those causing
 * libmseed parsing to fail should be detected.
 *
 * This routine is primarily intended to diagnose invalid miniSEED headers.
 *
 * @param[in] record Buffer to parse as miniSEED
 * @param[in] maxreclen Maximum length to search in buffer
 * @param[in] details Controls diagnostic output as follows:
 * @parblock
 *  - \c 0 - only print error messages for invalid header fields
 *  - \c 1 - print basic fields in addition to invalid field errors
 *  - \c 2 - print all fields in addition to invalid field errors
 * @endparblock
 * @param[in] swapflag Flag controlling byte-swapping as follows:
 * @parblock
 *  - \c 1 - swap multibyte quantities
 *  - \c 0 - do not swap
 *  - \c -1 - autodetect byte order using year test, swap if needed
 * @endparblock
 *
 * @returns 0 when no errors were detected or a positive count of
 * errors detected.
 *
 * \ref MessageOnError - this function logs a message on error
 ***************************************************************************/
int
ms_parse_raw2000 (char *record, int maxreclen, int8_t details, int8_t swapflag)
{
  double nomsamprate;
  char sid[25] = {0};
  char *X;
  uint8_t b;
  int retval = 0;
  int b1000encoding = -1;
  int b1000reclen = -1;
  int endofblockettes = -1;
  int idx;

  if (!record)
  {
    ms_log (2, "Required argument not defined: 'record'\n");
    return 1;
  }

  if (maxreclen < 48)
  {
    ms_log (2, "The maxreclen value cannot be smaller than 48\n");
    return 1;
  }

  /* Build source identifier for this record */
  ms2_recordsid (record, sid, sizeof (sid));

  /* Check to see if byte swapping is needed by testing the year and day */
  if (swapflag == -1 && !MS_ISVALIDYEARDAY (*pMS2FSDH_YEAR (record), *pMS2FSDH_DAY (record)))
    swapflag = 1;
  else
    swapflag = 0;

  if (details > 1)
  {
    if (swapflag == 1)
      ms_log (0, "Swapping multi-byte quantities in header\n");
    else
      ms_log (0, "Not swapping multi-byte quantities in header\n");
  }

  /* Validate fixed section header fields */
  X = record; /* Pointer of convenience */

  /* Check record sequence number, 6 ASCII digits */
  if (!isdigit ((int)*(X)) || !isdigit ((int)*(X + 1)) ||
      !isdigit ((int)*(X + 2)) || !isdigit ((int)*(X + 3)) ||
      !isdigit ((int)*(X + 4)) || !isdigit ((int)*(X + 5)))
  {
    ms_log (2, "%s: Invalid sequence number: '%c%c%c%c%c%c'\n",
            sid, *X, *(X + 1), *(X + 2), *(X + 3), *(X + 4), *(X + 5));
    retval++;
  }

  /* Check header data/quality indicator */
  if (!MS2_ISDATAINDICATOR (*(X + 6)))
  {
    ms_log (2, "%s: Invalid header indicator (DRQM): '%c'\n", sid, *(X + 6));
    retval++;
  }

  /* Check reserved byte, space or NULL */
  if (!(*(X + 7) == ' ' || *(X + 7) == '\0'))
  {
    ms_log (2, "%s: Invalid fixed section reserved byte (space): '%c'\n", sid, *(X + 7));
    retval++;
  }

  /* Check station code, 5 alphanumerics or spaces */
  if (!(isalnum ((unsigned char)*(X + 8)) || *(X + 8) == ' ') ||
      !(isalnum ((unsigned char)*(X + 9)) || *(X + 9) == ' ') ||
      !(isalnum ((unsigned char)*(X + 10)) || *(X + 10) == ' ') ||
      !(isalnum ((unsigned char)*(X + 11)) || *(X + 11) == ' ') ||
      !(isalnum ((unsigned char)*(X + 12)) || *(X + 12) == ' '))
  {
    ms_log (2, "%s: Invalid station code: '%c%c%c%c%c'\n",
            sid, *(X + 8), *(X + 9), *(X + 10), *(X + 11), *(X + 12));
    retval++;
  }

  /* Check location ID, 2 alphanumerics or spaces */
  if (!(isalnum ((unsigned char)*(X + 13)) || *(X + 13) == ' ') ||
      !(isalnum ((unsigned char)*(X + 14)) || *(X + 14) == ' '))
  {
    ms_log (2, "%s: Invalid location ID: '%c%c'\n", sid, *(X + 13), *(X + 14));
    retval++;
  }

  /* Check channel codes, 3 alphanumerics or spaces */
  if (!(isalnum ((unsigned char)*(X + 15)) || *(X + 15) == ' ') ||
      !(isalnum ((unsigned char)*(X + 16)) || *(X + 16) == ' ') ||
      !(isalnum ((unsigned char)*(X + 17)) || *(X + 17) == ' '))
  {
    ms_log (2, "%s: Invalid channel codes: '%c%c%c'\n", sid, *(X + 15), *(X + 16), *(X + 17));
    retval++;
  }

  /* Check network code, 2 alphanumerics or spaces */
  if (!(isalnum ((unsigned char)*(X + 18)) || *(X + 18) == ' ') ||
      !(isalnum ((unsigned char)*(X + 19)) || *(X + 19) == ' '))
  {
    ms_log (2, "%s: Invalid network code: '%c%c'\n", sid, *(X + 18), *(X + 19));
    retval++;
  }

  /* Check start time fields */
  if (HO2u(*pMS2FSDH_YEAR (record), swapflag) < 1900 || HO2u(*pMS2FSDH_YEAR (record), swapflag) > 2100)
  {
    ms_log (2, "%s: Unlikely start year (1900-2100): '%d'\n", sid, HO2u(*pMS2FSDH_YEAR (record), swapflag));
    retval++;
  }
  if (HO2u(*pMS2FSDH_DAY (record), swapflag) < 1 || HO2u(*pMS2FSDH_DAY (record), swapflag) > 366)
  {
    ms_log (2, "%s: Invalid start day (1-366): '%d'\n", sid, HO2u(*pMS2FSDH_DAY (record), swapflag));
    retval++;
  }
  if (*pMS2FSDH_HOUR (record) > 23)
  {
    ms_log (2, "%s: Invalid start hour (0-23): '%d'\n", sid, *pMS2FSDH_HOUR (record));
    retval++;
  }
  if (*pMS2FSDH_MIN (record) > 59)
  {
    ms_log (2, "%s: Invalid start minute (0-59): '%d'\n", sid, *pMS2FSDH_MIN (record));
    retval++;
  }
  if (*pMS2FSDH_SEC (record) > 60)
  {
    ms_log (2, "%s: Invalid start second (0-60): '%d'\n", sid, *pMS2FSDH_SEC (record));
    retval++;
  }
  if (HO2u(*pMS2FSDH_FSEC (record), swapflag) > 9999)
  {
    ms_log (2, "%s: Invalid start fractional seconds (0-9999): '%d'\n", sid, HO2u(*pMS2FSDH_FSEC (record), swapflag));
    retval++;
  }

  /* Check number of samples, max samples in 4096-byte Steim-2 encoded record: 6601 */
  if (HO2u(*pMS2FSDH_NUMSAMPLES(record), swapflag) > 20000)
  {
    ms_log (2, "%s: Unlikely number of samples (>20000): '%d'\n",
            sid, HO2u(*pMS2FSDH_NUMSAMPLES(record), swapflag));
    retval++;
  }

  /* Sanity check that there is space for blockettes when both data and blockettes are present */
  if (HO2u(*pMS2FSDH_NUMSAMPLES(record), swapflag) > 0 &&
      *pMS2FSDH_NUMBLOCKETTES(record) > 0 &&
      HO2u(*pMS2FSDH_DATAOFFSET(record), swapflag) <= HO2u(*pMS2FSDH_BLOCKETTEOFFSET(record), swapflag))
  {
    ms_log (2, "%s: No space for %d blockettes, data offset: %d, blockette offset: %d\n", sid,
            *pMS2FSDH_NUMBLOCKETTES(record),
            HO2u(*pMS2FSDH_DATAOFFSET(record), swapflag),
            HO2u(*pMS2FSDH_BLOCKETTEOFFSET(record), swapflag));
    retval++;
  }

  /* Print raw header details */
  if (details >= 1)
  {
    /* Determine nominal sample rate */
    nomsamprate = ms_nomsamprate (HO2d(*pMS2FSDH_SAMPLERATEFACT (record), swapflag),
                                  HO2d(*pMS2FSDH_SAMPLERATEMULT (record), swapflag));

    /* Print header values */
    ms_log (0, "RECORD -- %s\n", sid);
    ms_log (0, "        sequence number: '%c%c%c%c%c%c'\n",
            pMS2FSDH_SEQNUM (record)[0], pMS2FSDH_SEQNUM (record)[1],
            pMS2FSDH_SEQNUM (record)[2], pMS2FSDH_SEQNUM (record)[3],
            pMS2FSDH_SEQNUM (record)[4], pMS2FSDH_SEQNUM (record)[5]);
    ms_log (0, " data quality indicator: '%c'\n", *pMS2FSDH_DATAQUALITY (record));
    if (details > 0)
      ms_log (0, "               reserved: '%c'\n", *pMS2FSDH_RESERVED (record));
    ms_log (0, "           station code: '%c%c%c%c%c'\n",
            pMS2FSDH_STATION (record)[0], pMS2FSDH_STATION (record)[1],
            pMS2FSDH_STATION (record)[2], pMS2FSDH_STATION (record)[3], pMS2FSDH_STATION (record)[4]);
    ms_log (0, "            location ID: '%c%c'\n",
            pMS2FSDH_LOCATION (record)[0], pMS2FSDH_LOCATION (record)[1]);
    ms_log (0, "          channel codes: '%c%c%c'\n",
            pMS2FSDH_CHANNEL (record)[0], pMS2FSDH_CHANNEL (record)[1], pMS2FSDH_CHANNEL (record)[2]);
    ms_log (0, "           network code: '%c%c'\n",
            pMS2FSDH_NETWORK (record)[0], pMS2FSDH_NETWORK (record)[1]);
    ms_log (0, "             start time: %d,%d,%d:%d:%d.%04d (unused: %d)\n",
            HO2u(*pMS2FSDH_YEAR (record), swapflag),
            HO2u(*pMS2FSDH_DAY (record), swapflag),
            *pMS2FSDH_HOUR (record),
            *pMS2FSDH_MIN (record),
            *pMS2FSDH_SEC (record),
            HO2u(*pMS2FSDH_FSEC (record), swapflag),
            *pMS2FSDH_UNUSED (record));
    ms_log (0, "      number of samples: %d\n", HO2u(*pMS2FSDH_NUMSAMPLES (record), swapflag));
    ms_log (0, "     sample rate factor: %d  (%.10g samples per second)\n",
            HO2d(*pMS2FSDH_SAMPLERATEFACT (record), swapflag), nomsamprate);
    ms_log (0, " sample rate multiplier: %d\n", HO2d(*pMS2FSDH_SAMPLERATEMULT (record), swapflag));

    /* Print flag details if requested */
    if (details > 1)
    {
      /* Activity flags */
      b = *pMS2FSDH_ACTFLAGS (record);
      ms_log (0, "         activity flags: [%d%d%d%d%d%d%d%d] 8 bits\n",
              bit (b, 0x80), bit (b, 0x40), bit (b, 0x20), bit (b, 0x10),
              bit (b, 0x08), bit (b, 0x04), bit (b, 0x02), bit (b, 0x01));
      if (b & 0x01)
        ms_log (0, "                         [Bit 0] Calibration signals present\n");
      if (b & 0x02)
        ms_log (0, "                         [Bit 1] Time correction applied\n");
      if (b & 0x04)
        ms_log (0, "                         [Bit 2] Beginning of an event, station trigger\n");
      if (b & 0x08)
        ms_log (0, "                         [Bit 3] End of an event, station detrigger\n");
      if (b & 0x10)
        ms_log (0, "                         [Bit 4] A positive leap second happened in this record\n");
      if (b & 0x20)
        ms_log (0, "                         [Bit 5] A negative leap second happened in this record\n");
      if (b & 0x40)
        ms_log (0, "                         [Bit 6] Event in progress\n");
      if (b & 0x80)
        ms_log (0, "                         [Bit 7] Undefined bit set\n");

      /* I/O and clock flags */
      b = *pMS2FSDH_IOFLAGS (record);
      ms_log (0, "    I/O and clock flags: [%d%d%d%d%d%d%d%d] 8 bits\n",
              bit (b, 0x80), bit (b, 0x40), bit (b, 0x20), bit (b, 0x10),
              bit (b, 0x08), bit (b, 0x04), bit (b, 0x02), bit (b, 0x01));
      if (b & 0x01)
        ms_log (0, "                         [Bit 0] Station volume parity error possibly present\n");
      if (b & 0x02)
        ms_log (0, "                         [Bit 1] Long record read (possibly no problem)\n");
      if (b & 0x04)
        ms_log (0, "                         [Bit 2] Short record read (record padded)\n");
      if (b & 0x08)
        ms_log (0, "                         [Bit 3] Start of time series\n");
      if (b & 0x10)
        ms_log (0, "                         [Bit 4] End of time series\n");
      if (b & 0x20)
        ms_log (0, "                         [Bit 5] Clock locked\n");
      if (b & 0x40)
        ms_log (0, "                         [Bit 6] Undefined bit set\n");
      if (b & 0x80)
        ms_log (0, "                         [Bit 7] Undefined bit set\n");

      /* Data quality flags */
      b = *pMS2FSDH_DQFLAGS (record);
      ms_log (0, "     data quality flags: [%d%d%d%d%d%d%d%d] 8 bits\n",
              bit (b, 0x80), bit (b, 0x40), bit (b, 0x20), bit (b, 0x10),
              bit (b, 0x08), bit (b, 0x04), bit (b, 0x02), bit (b, 0x01));
      if (b & 0x01)
        ms_log (0, "                         [Bit 0] Amplifier saturation detected\n");
      if (b & 0x02)
        ms_log (0, "                         [Bit 1] Digitizer clipping detected\n");
      if (b & 0x04)
        ms_log (0, "                         [Bit 2] Spikes detected\n");
      if (b & 0x08)
        ms_log (0, "                         [Bit 3] Glitches detected\n");
      if (b & 0x10)
        ms_log (0, "                         [Bit 4] Missing/padded data present\n");
      if (b & 0x20)
        ms_log (0, "                         [Bit 5] Telemetry synchronization error\n");
      if (b & 0x40)
        ms_log (0, "                         [Bit 6] A digital filter may be charging\n");
      if (b & 0x80)
        ms_log (0, "                         [Bit 7] Time tag is questionable\n");
    }

    ms_log (0, "   number of blockettes: %d\n", *pMS2FSDH_NUMBLOCKETTES (record));
    ms_log (0, "        time correction: %ld\n", (long int)HO4d(*pMS2FSDH_TIMECORRECT (record), swapflag));
    ms_log (0, "            data offset: %d\n", HO2u(*pMS2FSDH_DATAOFFSET (record), swapflag));
    ms_log (0, " first blockette offset: %d\n", HO2u(*pMS2FSDH_BLOCKETTEOFFSET (record), swapflag));
  } /* Done printing raw header details */

  /* Validate and report information in the blockette chain */
  if (HO2u(*pMS2FSDH_BLOCKETTEOFFSET (record), swapflag) > 46 &&
      HO2u(*pMS2FSDH_BLOCKETTEOFFSET (record), swapflag) < maxreclen)
  {
    int blkt_offset = HO2u(*pMS2FSDH_BLOCKETTEOFFSET (record), swapflag);
    int blkt_count  = 0;
    int blkt_length;
    uint16_t blkt_type;
    uint16_t next_blkt;
    const char *blkt_desc;

    /* Traverse blockette chain */
    while (blkt_offset != 0 && blkt_offset < maxreclen)
    {
      /* Every blockette has a similar 4 byte header: type and next */
      memcpy (&blkt_type, record + blkt_offset, 2);
      memcpy (&next_blkt, record + blkt_offset + 2, 2);

      if (swapflag)
      {
        ms_gswap2 (&blkt_type);
        ms_gswap2 (&next_blkt);
      }

      /* Print common header fields */
      if (details >= 1)
      {
        blkt_desc = ms2_blktdesc (blkt_type);
        ms_log (0, "          BLOCKETTE %u: (%s)\n", blkt_type, (blkt_desc) ? blkt_desc : "Unknown");
        ms_log (0, "              next blockette: %u\n", next_blkt);
      }

      blkt_length = ms2_blktlen (blkt_type, record + blkt_offset, swapflag);
      if (blkt_length == 0)
      {
        ms_log (2, "%s: Unknown blockette length for type %d\n", sid, blkt_type);
        retval++;
      }

      /* Track end of blockette chain */
      endofblockettes = blkt_offset + blkt_length - 1;

      /* Sanity check that the blockette is contained in the record */
      if (endofblockettes > maxreclen)
      {
        ms_log (2, "%s: Blockette type %d at offset %d with length %d does not fit in record (%d)\n",
                sid, blkt_type, blkt_offset, blkt_length, maxreclen);
        retval++;
        break;
      }

      if (blkt_type == 1000)
      {
        char order[40];

        /* Calculate record size in bytes as 2^(blkt_1000->rec_len) */
        b1000reclen = (uint32_t)1 << *pMS2B1000_RECLEN (record + blkt_offset);

        /* Big or little endian? */
        if (*pMS2B1000_BYTEORDER (record + blkt_offset) == 0)
          strncpy (order, "Little endian", sizeof (order) - 1);
        else if (*pMS2B1000_BYTEORDER (record + blkt_offset) == 1)
          strncpy (order, "Big endian", sizeof (order) - 1);
        else
          strncpy (order, "Unknown value", sizeof (order) - 1);

        if (details >= 1)
        {
          ms_log (0, "                    encoding: %s (val:%u)\n",
                  (char *)ms_encodingstr (*pMS2B1000_ENCODING (record + blkt_offset)),
                  *pMS2B1000_ENCODING (record + blkt_offset));
          ms_log (0, "                  byte order: %s (val:%u)\n",
                  order, *pMS2B1000_BYTEORDER (record + blkt_offset));
          ms_log (0, "               record length: %d (val:%u)\n",
                  b1000reclen, *pMS2B1000_RECLEN (record + blkt_offset));

          if (details > 1)
            ms_log (0, "               reserved byte: %u\n", *pMS2B1000_RESERVED (record + blkt_offset));
        }

        /* Save encoding format */
        b1000encoding = *pMS2B1000_ENCODING (record + blkt_offset);

        /* Sanity check encoding format */
        if (!(b1000encoding >= 0 && b1000encoding <= 5) &&
            !(b1000encoding >= 10 && b1000encoding <= 19) &&
            !(b1000encoding >= 30 && b1000encoding <= 33))
        {
          ms_log (2, "%s: Blockette 1000 encoding format invalid (0-5,10-19,30-33): %d\n", sid, b1000encoding);
          retval++;
        }

        /* Sanity check byte order flag */
        if (*pMS2B1000_BYTEORDER (record + blkt_offset) != 0 &&
            *pMS2B1000_BYTEORDER (record + blkt_offset) != 1)
        {
          ms_log (2, "%s: Blockette 1000 byte order flag invalid (0 or 1): %d\n", sid,
                  *pMS2B1000_BYTEORDER (record + blkt_offset));
          retval++;
        }
      }

      else if (blkt_type == 1001)
      {
        if (details >= 1)
        {
          ms_log (0, "              timing quality: %u%%\n", *pMS2B1001_TIMINGQUALITY (record + blkt_offset));
          ms_log (0, "                micro second: %d\n", *pMS2B1001_MICROSECOND (record + blkt_offset));

          if (details > 1)
            ms_log (0, "               reserved byte: %u\n", *pMS2B1001_RESERVED (record + blkt_offset));

          ms_log (0, "                 frame count: %u\n", *pMS2B1001_FRAMECOUNT (record + blkt_offset));
        }
      }

      else if (blkt_type == 2000)
      {
        char order[40];

        /* Big or little endian? */
        if (*pMS2B2000_BYTEORDER (record + blkt_offset) == 0)
          strncpy (order, "Little endian", sizeof (order) - 1);
        else if (*pMS2B2000_BYTEORDER (record + blkt_offset) == 1)
          strncpy (order, "Big endian", sizeof (order) - 1);
        else
          strncpy (order, "Unknown value", sizeof (order) - 1);

        if (details >= 1)
        {
          ms_log (0, "            blockette length: %u\n", HO2u(*pMS2B2000_LENGTH (record + blkt_offset), swapflag));
          ms_log (0, "                 data offset: %u\n", HO2u(*pMS2B2000_DATAOFFSET (record + blkt_offset), swapflag));
          ms_log (0, "               record number: %u\n", HO4u(*pMS2B2000_RECNUM (record + blkt_offset), swapflag));
          ms_log (0, "                  byte order: %s (val:%u)\n",
                  order, *pMS2B2000_BYTEORDER (record + blkt_offset));
          b = *pMS2B2000_FLAGS (record + blkt_offset);
          ms_log (0, "                  data flags: [%d%d%d%d%d%d%d%d] 8 bits\n",
                  bit (b, 0x80), bit (b, 0x40), bit (b, 0x20), bit (b, 0x10),
                  bit (b, 0x08), bit (b, 0x04), bit (b, 0x02), bit (b, 0x01));

          if (details > 1)
          {
            if (b & 0x01)
              ms_log (0, "                         [Bit 0] 1: Stream oriented\n");
            else
              ms_log (0, "                         [Bit 0] 0: Record oriented\n");
            if (b & 0x02)
              ms_log (0, "                         [Bit 1] 1: Blockette 2000s may NOT be packaged\n");
            else
              ms_log (0, "                         [Bit 1] 0: Blockette 2000s may be packaged\n");
                if (!(b & 0x04) && !(b & 0x08))
              ms_log (0, "                      [Bits 2-3] 00: Complete blockette\n");
//            else if (!(b & 0x04) && (b & 0x08))
            else if ((b & 0x04) && !(b & 0x08))
              ms_log (0, "                      [Bits 2-3] 01: First blockette in span\n");
            else if ((b & 0x04) && (b & 0x08))
              ms_log (0, "                      [Bits 2-3] 11: Continuation blockette in span\n");
//            else if ((b & 0x04) && !(b & 0x08))
            else if (!(b & 0x04) && (b & 0x08))
              ms_log (0, "                      [Bits 2-3] 10: Final blockette in span\n");
            if (!(b & 0x10) && !(b & 0x20))
              ms_log (0, "                      [Bits 4-5] 00: Not file oriented\n");
            else if (!(b & 0x10) && (b & 0x20))
              ms_log (0, "                      [Bits 4-5] 01: First blockette of file\n");
            else if ((b & 0x10) && !(b & 0x20))
              ms_log (0, "                      [Bits 4-5] 10: Continuation of file\n");
            else if ((b & 0x10) && (b & 0x20))
              ms_log (0, "                      [Bits 4-5] 11: Last blockette of file\n");
          }

          ms_log (0, "           number of headers: %u\n", *pMS2B2000_NUMHEADERS (record + blkt_offset));

          /* Crude display of the opaque data headers, hopefully printable */
          if (details > 1)
            ms_log (0, "                     headers: %.*s\n",
                    (HO2u(*pMS2B2000_DATAOFFSET (record + blkt_offset), swapflag) - 15),
                    pMS2B2000_PAYLOAD (record + blkt_offset));
		  unsigned dataoffset=HO2u(*pMS2B2000_DATAOFFSET (record + blkt_offset),swapflag);
          unsigned len=HO2u(*pMS2B2000_LENGTH(record + blkt_offset),swapflag)-dataoffset;
          printf("payload(%d-bytes):%*.*s\n",len,len,len,record + blkt_offset+dataoffset);
        }

        unsigned dataoffset=HO2u(*pMS2B2000_DATAOFFSET (record + blkt_offset),swapflag);
        unsigned len=HO2u(*pMS2B2000_LENGTH(record + blkt_offset),swapflag)-dataoffset;
        blk2000pos+=snprintf(blk2000+blk2000pos, sizeof(blk2000)-blk2000pos,"%*.*s",len,len,record + blkt_offset+dataoffset);
      }

      else
      {
        ms_log (2, "%s: Unrecognized blockette type: %d\n", sid, blkt_type);
        retval++;
      }

      /* Sanity check the next blockette offset */
      if (next_blkt && next_blkt <= endofblockettes)
      {
        ms_log (2, "%s: Next blockette offset (%d) is within current blockette ending at byte %d\n",
                sid, next_blkt, endofblockettes);
        blkt_offset = 0;
      }
      else
      {
        blkt_offset = next_blkt;
      }

      blkt_count++;
    } /* End of looping through blockettes */

  }

  return retval;
} /* End of ms_parse_raw2() */

