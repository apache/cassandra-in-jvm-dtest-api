# Apache Cassandra in-JVM DTest API

Shared API package for in-JVM distributed tests.

# Publishing snapshot

```
mvn versions:set -DnewVersion=0.0.2-`git rev-parse --short HEAD`-SNAPSHOT
mvn deploy
```

# Releasing

1. Switch to `release` branch. This is useful maven release plugin pushes the current branch. Since we're voting
on the release, in cases when release is declined for some reason, we have to clean up history and prepare a new
release. Doing it through `release` branch makes it a bit easier:

```
git branch -D release
git checkout -b release
```

2. Prepare the release:

```
mvn release:clean
CURRENT=0.0.CURRENT
NEXT_DEV=0.0.NEXT
mvn -DreleaseVersion=$CURRENT -Dtag=$CURRENT -DdevelopmentVersion=$NEXT_DEV-SNAPSHOT release:prepare
mvn release:perform
```

3. Close staging repository: https://repository.apache.org/#stagingRepositories

4. Issue a vote on developers mailing list. Add your GPG key signature, release SHA, and staged artifacts to release information.

## Additional resources:

Parent pom location: https://maven.apache.org/pom/asf/
Maven distribution docs: http://www.apache.org/dev/publishing-maven-artifacts.html
Creating a new reposotory: https://selfserve.apache.org/

## GPG Key

To generate key, run:

```
gpg --full-gen-key
```

To be able to sign releases with this key, make sure your key is:

  * pushed to http://pool.sks-keyservers.net/

```
gpg --list-sigs "<YOUR_NAME>"
gpg --verbose --send-keys --keyserver hkps://hkps.pool.sks-keyservers.net <YOUR_KEY_IDENTIFIER_HERE>
# to test if it has worked:
gpg --verbose --recv-keys --keyserver hkps://hkps.pool.sks-keyservers.net <YOUR_KEY_IDENTIFIER_HERE>
```

  * added to KEYS file

```
svn co --depth files https://dist.apache.org/repos/dist/release/cassandra/ release
(gpg --list-sigs "<YOUR_NAME>" && gpg --armor --export "<YOUR_NAME>") >> KEYS
svn commit KEYS -m "Add <YOUR NAME>'s key for releases" # or ask some PMC to do this for you by opening CASSANDRA jira, like this one: https://issues.apache.org/jira/browse/CASSANDRA-15534
```