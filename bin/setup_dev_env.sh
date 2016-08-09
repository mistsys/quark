#!/bin/bash
download_dir=$(mktemp -d "${TMPDIR:-/tmp}"/spark-setup.XXXXX)
MODE=${1:-local}
DOWNLOAD_BASE=http://d3kbcqa49mib13.cloudfront.net
HADOOP_VERSION=2.6
INSTALL_DIR=${2:-_dependencies}
TMP_DIR="_tmp"
DEPS_FILE=${3:-deps}

if [ "$MODE" = "local" ]; then
    if [ ! -f $HOME/.aws/credentials ]; then
        echo "Your AWS credentials are not setup. Contact someone to set them up."
        echo "Follow the instructions here to setup credentials. http://docs.aws.amazon.com/cli/latest/userguide/cli-chap-getting-set-up.html"
        exit 1
    fi
fi

mkdir -p $INSTALL_DIR
mkdir -p $INSTALL_DIR/libs
mkdir -p $TMP_DIR/metrics $TMP_DIR/events
total_pkgs=$(cat $DEPS_FILE | wc -l)
installed_pkgs=0
if [ -f $INSTALL_DIR/setup-complete ]; then
    installed_pkgs=$(cat $INSTALL_DIR/setup-complete)
    # Yes there are corner cases, I'm not writing a full package manager
    if [ "$installed_pkgs" -eq "$total_pkgs" ]; then
        echo "You already have setup done, please delete $INSTALL_DIR/setup-complete and run"
    fi
fi
COUNTER=0
while read -r url
do
    if [ "$COUNTER" -ge $installed_pkgs ]; then
        filename=$(basename $url)
        filepath=$download_dir/$filename
        wget -O $filepath $url
        if [[ $filename == *.tgz ]] || [[ $filename == *.tar.gz ]] ; then
            tar -C $INSTALL_DIR -xzf $filepath
        elif [[ $filename == *.tar ]]; then
            tar -C $INSTALL_DIR -xzf $filepath
        elif [[ $filename == *.jar ]]; then
        cp $filepath $INSTALL_DIR/libs
        else
            cp $filepath $INSTALL_DIR
        fi
        ((installed_pkgs += 1))
    fi
    (( COUNTER += 1 ))
done < $DEPS_FILE
cat > $INSTALL_DIR/setup-complete<<EOF
$installed_pkgs
EOF

## Based on http://docs.python-guide.org/en/latest/dev/virtualenvs/
## Setup a virtualenv
if [ "x$SETUP_PYTHON_ENV" = "xtrue" ]; then
    type mkvirtualenv >/dev/null || install=true
    type workon >/dev/null || install=true
    if [ "$install" = "true" ]; then
        echo "No virtualenv setup, installing"
        curl -sL https://raw.githubusercontent.com/brainsik/virtualenv-burrito/master/virtualenv-burrito.sh | $SHELL
        #exit 1
    fi
    source ~/.venvburrito/startup.sh
    mkvirtualenv mist_spark
    workon mist_spark
    # switch to using requirements.txt
    pip install requests
    pip install pylint
    pip install jupyter
    pip install numpy
    pip install flake8
    pip install pandas
fi
