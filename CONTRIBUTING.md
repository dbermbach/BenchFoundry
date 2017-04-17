### Cloning the git repository

* Open a terminal (on Mac OS) or a command window (on Windows) and change directory to your workspace. On my machine I have chosen `/Users/akon/Documents/workspace` to host my local repository.
* Open your browser and enter `https://github.com/dbermbach/BenchFoundry` in the address/search bar to navigate to the *BenchFoundry* github repository.
* You should see the code and associated documentation.
* In order to begin working on the code, you must first *clone* the repository to a directory on your development machine. This is done using the `git clone` command.
* Click on the **Clone or download** button as seen in the following screenshot and copy the URL into your clipboard. 
* Now, go back to your terminal (or command) window and run the following command:
```
$ git clone git@github.com:dbermbach/BenchFoundry.git
Cloning into 'BenchFoundry'...
remote: Counting objects: 464, done.
remote: Total 464 (delta 0), reused 0 (delta 0), pack-reused 464
Receiving objects: 100% (464/464), 659.19 KiB | 0 bytes/s, done.
Resolving deltas: 100% (147/147), done.
Checking connectivity... done.
$
```
**Note: I am using ssh to clone the and communicate with github.** This is the prefered and more secure way. See the following instruction on how to setup ssh access to github [Generating an SSH key](https://help.github.com/articles/generating-an-ssh-key/). There are instructions for both Mac and Windows.
* The repository is cloned under the directory called `BenchFoundry` under the current directory. You can change directory to verify:
```
$ cd BenchFoundry
$ ls -a
.					log4j2.xml
..					mariadb.properties
.git					pom.xml
.gitignore				slaves.properties
README.md				src
benchfoundry.properties			teams.properties.example
benchfoundry_consbench_local.properties	tpcc
benchfoundry_minitest.properties
$
```

### Creating a Github issue
In order to track progress and document changes made to the system where developers are locates in multiple timezones across continents, it is important to make sure that we don't trample on each other's toes. There are two aspects to this. Using Github issues (bug tracking) and Git branching.

The process involves creating an issue for each piece of work to be performed. Writing down a description of the work to be performed. As work progresses on the issue, comments and notes are added to the issue. The following is a description of how an issue is created:

* Go to `https://github.com/dbermbach/BenchFoundry` and click on the **Issues** tab. 
* Next, click on the green **New issue** button to create a new issue. 
* Enter an appropriate title and related description of the issue. Label it appropriately and assign it to someone who will perform the task if it is known. 
* Once an issue is created you can find it in the list of issues when the **Issues** tab for the repo is clicked again.

### Making changes to the code
Before you start working on an issue, you must create a development branch in git off of the master branch, commit your changes to this branch and push it to the main github repository. This is followed by creating a pull request so that others can comment on the changes an suggest improvements. Once this is done, you can merge your changes to the master branch and relete the development branch.

The following is a description this process:
* Switch to the master branch on your local repository and pull the latest changes from the remote github repository to synchronize your local repository master branch with the remote repository.
```
$ git checkout master
Already on 'master'
Your branch is up-to-date with 'origin/master'.
$ git pull
Already up-to-date.
$ 
```
* Now that the repositories are in synch, create a branch in your local repository. Here I am creating a development branch for my work on an example. The branch tag is important, use a descriptive name and start with your name so that the creator and purpose of the branch is obvious from its name.
```
$ git checkout -b issue-5-create-contrib-guidelines
Switched to a new branch 'issue-5-create-contrib-guidelines'
$
```
* Make changes to the files you want to change or add/delete files as needed.
* Check the status of the files you changed using `git status` and check the difference using `git diff`. Here, I have added the *CONTRIBUTING.md* file where I have described the code contribution process for this project.
```
$ git status
On branch issue-5-create-contrib-guidelines
Untracked files:
  (use "git add <file>..." to include in what will be committed)

	CONTRIBUTING.md

nothing added to commit but untracked files present (use "git add" to track)
$ 
```
You can see the changes you have made using `git diff`.
* If you are happy with the change you can add these changes to a commit using `git add` as follows.
```
$ git add CONTRIBUTING.md
```
* Now, commit the change to your local repository:
```
$ git commit -m "Issue-5: Added code contribution process."
[issue-5-create-contrib-guidelines 3b16112] Issue-5: Added code contribution process.
 1 file changed, 112 insertions(+)
 create mode 100644 CONTRIBUTING.md
$
```
* Now that you have made the changes and committed to the developement branch on your local repository, you must push your changes to the remote repository so that the changes can be merged and made available to others. This is done using `git push`. If you just run `$ git push` you will get a message saying that the `push.default` is not set and inform you to run the following command to push and set the upstream. 
```
$ git push --set-upstream origin issue-5-create-contrib-guidelines
```
* Your changes are now aviailable in the github repository in a branch with the same name. Pointing your browser to `https://github.com/dbermbach/BenchFoundry` displays the newly created and pushed branch.
* Click on the **Compare and pull request** button to create a new pull request to merge your changes to the master branch.
* Add the appropriate pull request title and description and assign someone to review it then click the **Create pull request** button.
* The reviewer can comment on the changes and make suggestions for improvements. Once these have been completed satisfactorily, it is customary to mark it with a comment :+1: by using `:+1`.
* This is followed by merging the pull request by clicking the **Merge pull request**
followed by clicking **Confirm merge**
* Finally you can delete the development branch by clicking the **Delete branch** button.
