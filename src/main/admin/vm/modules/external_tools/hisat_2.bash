##depends:none

# TODO does this depend on functions.bash?
# Get wget_retry
source ../installation_files/functions.bash

cd ${TMPDIR_PATH}/
  # Hisat stuff

wget_retry -O hisat2-2.0.5-Linux_x86_64.zip ftp://ftp.ccb.jhu.edu/pub/infphilo/hisat2/downloads/hisat2-2.0.5-Linux_x86_64.zip
unzip hisat2-2.0.5-Linux_x86_64.zip -d ${TOOLS_PATH}/
rm -f hisat2-2.0.5-Linux_x86_64.zip 
ln -s hisat2-2.0.5-Linux_x86_64 ${TOOLS_PATH}/hisat2
