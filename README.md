# ZFSBackup 
This is a fairly simple python3 program to do one way sends of zfs datasets for backup purposes.
It makes a bunch of assumptions that you probably won't like, but I wrote it for my own internal use.
It is still very much a work in progress and probably won't evolve to the point where it can be used
as a product outside of my environment. There are some tests, there probably should be more. It seems
to be mostly functional at this point.
## Requirements
- Python (>=3.6)
- ZFS (ZoL >=0.7.0)
- PyYAML
## Features
- Support for local and remote sends
- Incremental sends (Wow!)
- Config file (Amazing!)
- Manual dataset backup
- The ability to be run by cron on some sort of schedule (Be still my beating heart!)
- Multiple backup targets per dataset
- Tunable snapshot deletion on the destination
## Planned Features
- More user tunable parameters
- Support for specific ssh keys (now it just assumes ssh <hostname> will work)
- Probably More
## Motivation
I wanted to write my own simple zfs backup program. There are better more complex offerings out there if you
have your own need.
## Future
I know there are plans to update pyzfs so I'll probably use that in the future. I also want to make this more OO.
