<h1>Cassiopeia NGSI-LD Broker</h1>

An ongoing project to implement a light-weight and fast NGSI-LD broker in TypeScript, running on Node.js and using PostgreSQL + PostGIS as storage back-end.

# Table of Contents
  - 1.) [License Information](#1-license-information)
  - 2.) [Overview](#2-overview)
  - 3.) [A Note About Current Limitations](#3-a-note-about-current-limitations)
  - 4.) [Project Context and Funding](#4-project-context-and-funding)
  - 5.) [How Cassiopeia is related to our former 'Hydra' project](#5-how-cassiopeia-is-related-to-our-former-hydra-project)
  - 6.) [Installing Cassiopeia](#6-installing-cassiopeia)
    - 6.1.) [Prerequisites](#61-prerequisites)
    - 6.2.) [Installing required .deb packages](#62-installing-required-deb-packages)
    - 6.3.) [Installing the TypeScript compiler globally](#63-installing-the-typescript-compiler-globally)
    - 6.4.) [Cloning Cassiopeia's Git repository](#64-cloning-cassiopeias-git-repository)
    - 6.5.) [Preparing the PostgreSQL Database](#65-preparing-the-postgresql-database)
    - 6.6.) [Installing required npm packages and compiling Cassiopeia's TypeScript code to JavaScript](#66-installing-required-npm-packages-and-compiling-cassiopeias-typescript-code-to-javascript)
    - 6.7.) [Modify Cassiopeia's configuration file](#67-modify-cassiopeias-configuration-file)
      - 6.7.1.) [Settings under "psql":](#671-settings-under-psql)
      - 6.7.2.) [Settings under "users":](#672-settings-under-users)
      - 6.7.3.) [Setting "port:](#673-setting-port)
    - 6.8.) [Starting the broker](#68-starting-the-broker)
    - 6.9.) [Unit Tests](#69-unit-tests)

# 1. License Information

(Beginning of License Text)

*Copyright 2021 GeoNet.MRN e.V.*

*Permission is hereby granted, free of charge, to any person obtaining a copy of this software and associated documentation files (the "Software"), to deal in the Software without restriction, including without limitation the rights to use, copy, modify, merge, publish, distribute, sublicense, and/or sell copies of the Software, and to permit persons to whom the Software is furnished to do so, subject to the following conditions:*

*The above copyright notice and this permission notice shall be included in all copies or substantial portions of the Software.*

*THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.*

(End of License Text)

# 2. Overview

NGSI-LD is a RESTful API for web services and distributed application, primarily designed with a focus on “Internet of Things” (IoT), “right-time” sensor data and “smart cities”. NGSI-LD is developed by the FIWARE Foundation and formally standardized by the European Telecommunications Standards Institute (ETSI). On the ETSI website, you can find the official specification document for the latest version of NGSI-LD:

https://www.etsi.org/deliver/etsi_gs/CIM/001_099/009/01.04.01_60/gs_CIM009v010401p.pdf

Cassiopeia is one of several software applications which partially or fully implement the NGSI-LD specification. Such an application is called a *NGSI-LD broker*. Expressed in a simple way, an NGSI-LD broker is basically a NoSQL database with an integrated HTTP interface, where for the structure of the stored data, as well as for the design of the HTTP interface, the rules defined in the NGSI-LD specification apply.

The Cassiopeia project currently aims to implement version 1.3.1 of the NGSI-LD specification.


# 3. A Note About Current Limitations

Please note that Cassiopeia is still in a very early stage of development. It does currently not implement the full NGSI-LD specification. However, it does already implement what we think are the most important parts of it, including support for spatial (NGSI-LD Geo-query language) and temporal (NGSI-LD Temporal Query Language) queries. If you need a more stable and complete NGSI-LD broker, consider using **Scorpio** (https://github.com/ScorpioBroker/ScorpioBroker), **Stellio** (https://github.com/stellio-hub/stellio-context-broker) or **Orion-LD** (https://github.com/FIWARE/context.Orion-LD).

**PLEASE DO NOT RUN CASSIOPEIA IN A PRODUCTION ENVIRONMENT! EXPECT BUGS, SECURITY ISSUES AND DEVIATIONS FROM THE NGSI-LD SPECIFICATION! ALSO, READ THE LICENSE INFORMATION CAREFULLY!**


# 4. Project Context and Funding

Cassiopeia is developed by GeoNet.MRN e.V. as part of xDataToGO, a cooperative research project funded by the Germany Ministry
of Transport and Digital Infrastructure (BMVI) as part of the mFund initiative. Goal of the xDataToGo project is to develop new
methods and digital solutions to find, collect and make available data about the public street space, specifically to improve and
simplify planning of large-volume and heavy goods transport.


# 5. How Cassiopeia is related to our former 'Hydra' project

The Hydra project was our first attempt to implement our own NGSI-LD broker. Its primary goal was to learn the NGSI-LD protocol by implementing it. Python was chosen as the programming language for Hydra, and PostgreSQL + PostGIS as the storage back-end, just like now with Cassiopeia. The Hydra project was eventually terminated for two reasons: 

First, we wanted to switch to TypeScript because by design of the language (strict type definitions, resulting in definite function and variable signatures), there are much better code analysis and refactoring tools available for TypeScript compared to Python. For example, something like *"Rename this variable in all source files of the project"* (based on code structure analysis and not dump string replacement) is something no Python IDE can do due to language-immanent ambiguity.

Second, since we were still at the very beginning of the long road of understanding NGSI-LD when we started with the implementation of Hydra, we made a lot of mistakes with it. Especially in terms of performance, the software design of Hydra was a dead end and would have required fundamental changes to get fixed.

Even though Cassiopeia is still in a very early stage of development, it is already a *much* better and much more complete NGSI-LD broker than Hydra ever was. Most importantly, it has a *way* better PostgreSQL database design and much more sophisticated SQL query generation. In Hydra, a lot of NGSI-LD query-related filtering of entities happened on the Python code level after an unnecessarily broad selection of entities was fetched from the PSQL database in a first step. Obviously, this was *slow*, and nothing that could ever be used for serious applications. Cassiopeia, on the other hand, does all filtering in SQL code, resulting in massively better performance.

So, as a summary: Hydra was a NGSI-LD "playground" and "exploration mission". Cassiopeia is now a serious attempt to implement a "real" NGSI-LD broker, built on top of the things that were learned with Hydra.


# 6. Installing Cassiopeia


## 6.1. Prerequisites

The following installation instructions for Cassiopeia apply to Ubuntu Linux, tested with versions 18.04 and 20.04. For other operating systems, the procedure is probably very similar, but there might be some differences.

## 6.2. Installing required .deb packages

```
sudo apt install git postgresql postgis nodejs npm
```

## 6.3. Installing the TypeScript compiler globally

This step is required to compile Cassiopeia's TypeScript source files to JavaScript. Enter the following command in your shell:

```
sudo npm install -g typescript
```

## 6.4. Cloning Cassiopeia's Git repository

```
git clone https://github.com/geonet-mrn/cassiopeia-ngsi-ld-broker.git
```

## 6.5. Preparing the PostgreSQL Database

This step of the installation process requires some manual work on the PostgreSQL command line.
Start the PostgreSQL command line client with the following shell command:

```
sudo -u postgres psql
```

You must do this as a Linux user with "sudo" (administrator) rights. 
When you are asked for a password, enter your linux user account password.

If successful, you'll be greeted with a message similar to the following one:

```
psql (12.6 (Ubuntu 12.6-0ubuntu0.20.04.1))
Type "help" for help.

postgres=# 
```

Now, create a database for Cassiopeia with the following command:
 
```               
create database cassiopeia;
```

Next, create a new PostgreSQL user role for Cassiopeia. For security reasons, we don't want to have Cassiopeia access our
PostgreSQL server with the default "postgres" superuser account. For 'MY_PASSWORD', enter a secure password and make sure to keep it in a safe place:

```
create user cassiopeia with encrypted password 'MY_PASSWORD';
```
Finally, we grant our newly created user/role all privileges on the newly created database:

```
grant all privileges on database cassiopeia to cassiopeia;
```

We are now done here and exit the PostgreSQL command line client:

```
exit
```

With our newly created role and database in place, we now initialize the database by running the SQL file 'db_init.sql' that is included in the cloned Git repository folder:

```
sudo -u postgres psql -d cassiopeia -f db_init.sql

```

That's it. Preparation of the PostgreSQL database is now complete.


## 6.6. Installing required npm packages and compiling Cassiopeia's TypeScript code to JavaScript

On the bash command line, navigate to the folder containing the cloned Git repository.

Enter the following command to download the required npm packages:

```
npm install
```

Next, enter the following command to compile Cassiopeia's TypeScript source code to JavaScript:

```
tsc
```

Note that that `tsc` command might throw the following errors:

```
- error TS2304: Cannot find name 'RequestInit'.
- error TS2304: Cannot find name 'Response'.
```

This is a known problem. These error messages are caused by missing TypeScript type definitions in a 3rd party library that is used by Cassiopeia to process JSON-LD context definitions. However, this is only relevant for consistency checks of the code base and has no effect on the correctness of the produced JavaScript code. Unless you see more/other error messages, your compilation result is probably fine.

A new folder named "dist" should have appeared in the Cassiopeia directory. It contains the JavaScript files which were generated by the TypeScript compiler.


## 6.7. Modify Cassiopeia's configuration file

The file `cassiopeia_config.json` holds various settings that must be adjusted for Cassiopeia to work. If you have followed the installation steps exactly as described up to this point, the only value you need to change in this file is the PostgreSQL password. All other settings can be left as they are. However, if you e.g. decided to use a different name for your PostgreSQL database or role, you might have to make the configuration file reflect these changes.

Open `cassiopeia_config.json` with your text editor of choice and adjust its content as follows:

### 6.7.1. Settings under "psql":

- "host" : The host name or IP address of the machine your PostgreSQL server runs on. Default is "localhost".

- "port": The port your PostgreSQL server listens on. The default port for PostgreSQL is 5432.

- "database" : The name of the PostgreSQL database you have created for Cassiopeia. Default is "cassiopeia".

- "user" : The name of the PostgreSQL role for Cassiopeia which you have created earlier. Default is "cassiopeia".

- "password": The password you have set for the PostgreSQL role "cassiopeia" (represented by the placeholder "MY_PASSWORD" in the instructions above)


### 6.7.2. Settings under "users":

The pairs of username and password specified here represent the HTTP Basic Authentication credentials which can be used to perform write operations on the broker. 

Currently, Cassiopeia's user rights management system is extremely limited. NGSI-LD itself does not specify anything about how user rights should be defined and checked. Since this is not practical for most "real world" use cases, we have decided to implement our own solution for this. Currently, it only distinguishes between two types of actions: Full read and full write. It is currently not possible to specify more fine-grained access rules, like allowing/disallowing the use of different API operations or read/write access to individual entities on a per-user basis. 

As far as access control is currently implemented, reading is open for everybody, including anonymous users. Any write operation (i.e. creating, modifying or deleting entities and attributes) requires the user to provide one of the username/password pairs specified in the cassiopeia_config.json file as a HTTP Basic Authentication header in the HTTP request.

### 6.7.3. Setting "port:

This setting defines the network port on which Cassiopeia is listening. The default value is 3000.


## 6.8. Starting the broker

Cassiopeia is now ready to run. Move your command line working directory to the cloned repository folder and enter the following command:

```
node .
```

This will start a Node.js instance with the "index.js" file in the Cassiopeia directory as entry point.

If everything works as expected, you should see the following message in your command line window:

```
Cassiopeia NGSI-LD Context Broker started. NGSI-LD version 1.3.1.
```

## 6.9. Unit Tests

Cassiopeia comes with a couple of unit tests which can be used to check whether it works as expected. **Please note that unit test coverage of Cassiopeia is still in a very early stage of development**. At the moment, only a few tests are implemented, and they are not very well designed. It's better than nothing, but far from complete.

**ATTENTION: DO NOT RUN THE UNIT TESTS ON AN INSTANCE OF CASSIOPEIA THAT CONTAINS ANY IMPORTANT DATA! THE UNIT TESTS WILL DELETE ALL ENTITIES STORED IN THE BROKER!**


To run the unit tests, make sure that you have a instance of Cassiopeia running.

In the file `tests/testConfig.ts`, you can set various configuration variables related to unit testing. Make sure that the variable `base_url` points to your running Cassiopeia instance and that `username` and `password` match the credentials defined in your `cassiopeia_config.json` file.

When you have everything set up, you can run the unit tests with the following command from within the cloned repository directory:

```npm run test```

The tests might need a couple of seconds to run. If everything goes according to plan, you should see something similar to the following as the last line of output after the tests are completed:

```
7 passing (354ms)
```

Note that the actual number might vary depending on whether additional tests have been added since this document was written. In any case, there should be no mentions of failed tests. If there are, please first check your test configuration. If you then still think that a failure is caused by a bug in Cassiopeia's source code, please tell us about it.
