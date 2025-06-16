#ifndef TAR_TYPE_H
#define TAR_TYPE_H

#define REGTYPE '0'   /* regular file */
#define AREGTYPE '\0' /* regular file */
#define LNKTYPE '1'   /* link */
#define SYMTYPE '2'   /* reserved */
#define CHRTYPE '3'   /* character special */
#define BLKTYPE '4'   /* block special */
#define DIRTYPE '5'   /* directory */
#define FIFOTYPE '6'  /* FIFO special */
#define CONTTYPE '7'  /* reserved */
#define XHDTYPE 'x'   /* Extended header referring to the next file in the archive */
#define XGLTYPE 'g'   /* Global extended header */
#define GNUTYPE_DUMPDIR 'D'
/* Identifies the *next* file on the tape as having a long linkname.  */
#define GNUTYPE_LONGLINK 'K'
/* Identifies the *next* file on the tape as having a long name.  */
#define GNUTYPE_LONGNAME 'L'
/* This is the continuation of a file that began on another volume.  */
#define GNUTYPE_MULTIVOL 'M'
/* This is for sparse files.  */
#define GNUTYPE_SPARSE 'S'
/* This file is a tape/volume header.  Ignore it on extraction.  */
#define GNUTYPE_VOLHDR 'V'
/* Solaris extended header */
#define SOLARIS_XHDTYPE 'X'
/* J@"org Schilling star header */

#endif