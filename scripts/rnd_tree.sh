#!/usr/bin/env bash

# Adopted from http://stackoverflow.com/a/19878455

# http://stackoverflow.com/questions/13400312/linux-create-random-directory-file-hierarchy
# Decimal ASCII codes (see man ascii); added space
AARR=( 32 {48..57} {65..90} {97..122} )
# Array count
aarrcount=${#AARR[@]}

if [ "$1" == "" ] ; then
  OUTDIR="./rnd_tree" ;
else
  OUTDIR="$1" ;
fi

if [ "$2" != "" ] ; then
  ASCIIONLY="$2" ;
else
  ASCIIONLY=1 ;
fi

if [ "$3" != "" ] ; then
  DIR_DEPTH="$3" ;
else
  DIR_DEPTH=3 ;
fi

if [ "$4" != "" ] ; then
  MAX_FIRST_LEVEL_DIRS="$4" ;
else
  MAX_FIRST_LEVEL_DIRS=2 ;
fi

if [ "$5" != "" ] ; then
  MAX_DIR_CHILDREN="$5" ;
else
  MAX_DIR_CHILDREN=4 ;
fi

if [ "$6" != "" ] ; then
  MAX_DIR_NAME_LEN="$6" ;
else
  MAX_DIR_NAME_LEN=12 ;
fi

if [ "$7" != "" ] ; then
  MAX_FILE_CHILDREN="$7" ;
else
  MAX_FILE_CHILDREN=4 ;
fi

if [ "$8" != "" ] ; then
  MAX_FILE_NAME_LEN="$8" ;
else
  MAX_FILE_NAME_LEN=20 ;
fi

if [ "$9" != "" ] ; then
  MAX_FILE_SIZE="$9" ;
else
  MAX_FILE_SIZE=20000 ;
fi

MIN_DIR_NAME_LEN=1
MIN_FILE_NAME_LEN=1
MIN_DIR_CHILDREN=1
MIN_FILE_CHILDREN=0
MIN_FILE_SIZE=1
FILE_EXT=".bin"
VERBOSE=0 #1

get_rand_dirname() {
  if [ "$ASCIIONLY" == "1" ]; then
    for ((i=0; i<$((MIN_DIR_NAME_LEN+RANDOM%MAX_DIR_NAME_LEN)); i++)) {
      printf \\$(printf '%03o' ${AARR[RANDOM%aarrcount]});
    }
  else
    cat /dev/urandom | tr -dc '[ -~]' | tr -d '[$></~:`\\]' | head -c$((MIN_DIR_NAME_LEN + RANDOM % MAX_DIR_NAME_LEN)) | sed 's/\(["]\)/\\\1/g'
  fi
  #echo -e " " # debug last dirname space
}

get_rand_filename() {
  if [ "$ASCIIONLY" == "1" ]; then
    for ((i=0; i<$((MIN_FILE_NAME_LEN+RANDOM%MAX_FILE_NAME_LEN)); i++)) {
      printf \\$(printf '%03o' ${AARR[RANDOM%aarrcount]});
    }
  else
    # no need to escape double quotes for filename
    cat /dev/urandom | tr -dc '[ -~]' | tr -d '[$></~:`\\]' | head -c$((MIN_FILE_NAME_LEN + RANDOM % MAX_FILE_NAME_LEN)) #| sed 's/\(["]\)/\\\1/g'
  fi
  printf "%s" $FILE_EXT
}


echo "Warning: will create random tree at: $OUTDIR"
[ "$VERBOSE" == "1" ] && echo "  MAX_FIRST_LEVEL_DIRS $MAX_FIRST_LEVEL_DIRS ASCIIONLY $ASCIIONLY DIR_DEPTH $DIR_DEPTH MAX_DIR_CHILDREN $MAX_DIR_CHILDREN MAX_DIR_NAME_LEN $MAX_DIR_NAME_LEN MAX_FILE_CHILDREN $MAX_FILE_CHILDREN MAX_FILE_NAME_LEN $MAX_FILE_NAME_LEN MAX_FILE_SIZE $MAX_FILE_SIZE"

read -p "Proceed (y/n)? " READANS
if [ "$READANS" != "y" ]; then
  exit
fi

if [ -d "$OUTDIR" ]; then
  echo "Removing old outdir $OUTDIR"
  rm -rf "$OUTDIR"
fi

mkdir "$OUTDIR"

if [ $MAX_FIRST_LEVEL_DIRS -gt 0 ]; then
  NUM_FIRST_LEVEL_DIRS=$((1+RANDOM%MAX_FIRST_LEVEL_DIRS))
else
  NUM_FIRST_LEVEL_DIRS=0
fi



# create directories
for (( ifl=0;ifl<$((NUM_FIRST_LEVEL_DIRS));ifl++ )) {
  FLDIR="$(get_rand_dirname)"
  FLCHILDREN="";
  for (( ird=0;ird<$((DIR_DEPTH-1));ird++ )) {
    DIRCHILDREN=""; MOREDC=0;
    for ((idc=0; idc<$((MIN_DIR_CHILDREN+RANDOM%MAX_DIR_CHILDREN)); idc++)) {
      CDIR="$(get_rand_dirname)" ;
      # make sure comma is last, so brace expansion works even for 1 element? that can mess with expansion math, though
      if [ "$DIRCHILDREN" == "" ]; then DIRCHILDREN="\"$CDIR\"" ;
      else DIRCHILDREN="$DIRCHILDREN,\"$CDIR\"" ; MOREDC=1 ; fi
    }
    if [ "$MOREDC" == "1" ] ; then
      if [ "$FLCHILDREN" == "" ]; then FLCHILDREN="{$DIRCHILDREN}" ;
      else FLCHILDREN="$FLCHILDREN/{$DIRCHILDREN}" ; fi
    else
      if [ "$FLCHILDREN" == "" ]; then FLCHILDREN="$DIRCHILDREN" ;
      else FLCHILDREN="$FLCHILDREN/$DIRCHILDREN" ; fi
    fi
  }
  DIRCMD="mkdir -p $OUTDIR/\"$FLDIR\"/$FLCHILDREN"
  eval "$DIRCMD"
  echo "$DIRCMD"
}

# now loop through all directories, create random files inside
# note printf '%q' escapes to preserve spaces; also here
# escape, and don't wrap path parts in double quotes (e.g. | sed 's_/_"/"_g');
# note then we STILL have to eval to use it!
# but now ls "$D" works, so noneed for QD
# unfortunately backslashes can make '%q' barf - prevent them
find "$OUTDIR" -type d | while IFS= read D ; do
  QD="$(printf '%q' "$(echo "$D")" )" ;
  [ "$VERBOSE" == "1" ] && echo "$D"; #echo "$QD"; ls -la "$D"; #eval "ls -la $QD";
  for ((ifc=0; ifc<$((MIN_FILE_CHILDREN+RANDOM%MAX_FILE_CHILDREN)); ifc++)) {
    CFILE="$(get_rand_filename)" ;
    echo -n '> '
    [ "$VERBOSE" == "1" ] && echo "$D"/"$CFILE"
    cat /dev/urandom \
    | head -c$((MIN_FILE_SIZE + RANDOM % MAX_FILE_SIZE)) \
    > "$D"/"$CFILE"
  }
done

echo
echo "total bytes: $(du -bs $(echo "$OUTDIR"))"
