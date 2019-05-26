# Contributing to Broadway

First of all thank you for contributing to Broadway! :tada:

### How can I contribute?
There are many ways in which you can contribute. Broadway has a lot of sister open source projects which need maintenance too.
* [Broadway On-Demand](https://github.com/illinois-cs241/broadway-on-demand)
* [Broadway Graders](https://github.com/illinois-cs241/broadway-grader)
* [Chainlink](https://github.com/illinois-cs241/chainlink)

If you find issues in any of these, please feel free to make Issues or Pull Requests. Reporting issues itself is contributing!

### Issues
Please make issues as detailed as possible. Include error messages, logs and output in code blocks. If you are making feature requests
please explain in detail what purpose the feature would serve. **Please keep in mind that Broadway should not contain course-specific 
features or code.** Broadway's design has been kept generic for all courses to use and provide the flexibility to use it. Consider building
services around Broadway if you want course specific changes.

### Pull Requests
Please keep PRs as small as possible so its easier for reviewers to review it and get back to you. If you have a lot of changes,
try splitting them up into smaller parts and stacking PRs. It makes a world of a difference for reviewers. It will also be easier
spot potential errors.

Please keep meaningful commit messages, branch names and description. Summarize your changes in the description. Link all the issues
your PR is attemption to fix. Possibly prepend the issues links with "Resolves " so that the issue is immediately close when the PR
is merged to `master`.

**Please squash and merge** so that the commit history on master looks cleaner and easier to navigate.

### Blocking calls
Please be cautious of adding blocking calls in the application logic because Tornado uses a single-threaded event loop. Therefore, 
one blocking call will prevent the API from serving requests and hence tamper with the entire distributed system. For instance, 
it might prevent the API from listening to heartbeats and as a result the server will consider worker nodes to be dead.

If you want to use blocking calls, please make them asynchronous. [Asynchronous and non-blocking IO guide for Tornado](http://www.tornadoweb.org/en/stable/guide/async.html)

### Response Time
Almost all maintainers of this project are full-time students. We tend to get busy with college schedule and studies of our own. Please
be patient while we review your contributions. We would definitely get back to you because we want to improve Broadway as much as you do!
