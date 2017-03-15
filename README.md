# Enron Emails - Slack SLI Technical assignment

## Local Development Setup

### Pre-requisite
You need to have docker, docker-compose installed on your local machine.

### Install Forqlift
[Forqlift](http://www.exmachinatech.net/projects/forqlift/) is a tool for converting plain text files to sequence files. HDFS (and thus spark) does not work well with lots of small files, so sequence files are used instead.

To install forqlift, simply [download](http://www.exmachinatech.net/projects/forqlift/download/) the binaries and extract them. Add `$FORQLIFT/bin` to your `PATH` and you are ready to run `forqlift`.

### Data preparation

## Step 1 - Compile the full email corpus into a SequenceFile

The initial enron email data set can be found [here](https://www.cs.cmu.edu/~./enron/enron_mail_20150507.tgz). This compressed file (~423MB) contains plain text emails.

    wget "https://www.cs.cmu.edu/~./enron/enron_mail_20150507.tgz" ./resources/enron_mail_20150507.seq

## Step 2 - Compile the full email corpus into a SequenceFile

Use `forqlift` to create a sequence file:

    forqlift fromarchive ./resources/enron_mail_20150507.tgz --file ./resources/enron_mail_20150507.seq --compress bzip2 --data-type text

### Run computation

Simply run:

    docker-compose build spark && docker-compose run -e SOURCE_FILE=/resources/enron_mail_20150507.seq spark