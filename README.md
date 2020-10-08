# Apache Cassandra in-JVM DTest API

Shared API package for in-JVM distributed tests.

# Contributing

Modifications to this repository should be made when the in-jvm dtest
API dtests needs to be extended or modified. Modifications affect all
active Cassandra branches, so any changes should be built/tested against each branch.

## Installing a Local Snapshot

Running the following will install the in-jvm dtest api JAR into your local maven (default: `~/.m2`) repository.

```
mvn install
```

Once installed, the `build.xml` file on each Cassandra branch can be
modified to point to the SNAPSHOT version. When preparing a Cassandra branch for a new in-jvm dtest API release that
has not yet been voted on, it can be useful to symlink files in the local maven repository to simulate the soon
to be released version. When doing so, `maven-metadata-local.xml`, `dtest-api-VERSION.pom` and `dtest-api-VERSION.jar` are
required. For example:

```
$ ln -s ~/.m2/repository/org/apache/cassandra/dtest-api/0.0.5-SNAPSHOT/maven-metadata-local.xml ~/.m2/repository/org/apache/cassandra/dtest-api/0.0.6/maven-metadata-local.xml

$ ln -s ~/.m2/repository/org/apache/cassandra/dtest-api/0.0.5-SNAPSHOT/dtest-api-0.0.5-SNAPSHOT.pom ~/.m2/repository/org/apache/cassandra/dtest-api/0.0.6/dtest-api-0.0.6.pom

$ ln -s ~/.m2/repository/org/apache/cassandra/dtest-api/0.0.5-SNAPSHOT/dtest-api-0.0.5-SNAPSHOT.jar ~/.m2/repository/org/apache/cassandra/dtest-api/0.0.6/dtest-api-0.0.6.jar
```

Remember to clean up after this as it will stomp on the real release once it has been published. 

# Publishing snapshot

```
mvn versions:set -DnewVersion=0.0.2-`git rev-parse --short HEAD`-SNAPSHOT
mvn deploy
```

# Releasing

1. Prepare the release:

```
mvn release:clean
CURRENT=0.0.CURRENT
NEXT_DEV=0.0.NEXT
mvn -DreleaseVersion=$CURRENT -Dtag=$CURRENT -DdevelopmentVersion=$NEXT_DEV-SNAPSHOT release:prepare
mvn release:perform
```

2. Close staging repository: https://repository.apache.org/#stagingRepositories

3. Issue a vote on developers mailing list. Add your GPG key signature, release SHA, and staged artifacts to release information.

## Additional resources:

Parent pom location: https://maven.apache.org/pom/asf/
Maven distribution docs: http://www.apache.org/dev/publishing-maven-artifacts.html
Creating a new reposotory: https://selfserve.apache.org/

## GPG Key

When releasing for the first time, its necessary to generate a GPG key. Run the following:

```
gpg --full-gen-key
```

Before releasing your key should be:

  * added to KEYS file in Cassandra. The patch should be uploaded to a new JIRA to be approved and merged by
  someone with commit access to SVN. See CASSANDRA-15534 and CASSANDRA-16194 for more examples

```
svn co --depth files https://dist.apache.org/repos/dist/release/cassandra/ release
(gpg --list-sigs "<YOUR_NAME>" && gpg --armor --export "<YOUR_NAME>") >> KEYS
svn diff > my-key.patch
```

  * pushed to http://pool.sks-keyservers.net/

```
gpg --list-sigs "<YOUR_NAME>"
gpg --verbose --send-keys --keyserver hkps://hkps.pool.sks-keyservers.net <YOUR_KEY_IDENTIFIER_HERE>
# to test if it has worked:
gpg --verbose --recv-keys --keyserver hkps://hkps.pool.sks-keyservers.net <YOUR_KEY_IDENTIFIER_HERE>
```

If those servers fail, the `hkp://` protocol can be tried instead:

```
gpg --verbose --send-keys --keyserver hkp://hkps.pool.sks-keyservers.net <YOUR_KEY_IDENTIFIER_HERE>
```

In the case that `mvn release:prepare` fails to launch the GPG TTY for your key's password, set `export GPG_TTY=$(tty)`
in the current shell session or globally in `~/.bashrc` or similar. 

