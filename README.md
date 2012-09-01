For more information, visit http://www.systemical.com/doc/opensource/jldaws


Tests
=====

*nose* can be used to run tests.

Current work items
==================



History
=======

0.4.13: added 'write done file' to jlds3upload

0.4.7: enhancements to 'jldsdbget'

0.4.6: fixed bug in 'jldsdbget'

0.4.4: added 'jldsdbbatchinsert', enhanced logging across all scripts

0.4.2: added regex & format options to jlds3upload

0.4.1: added "jldsdb", "jldsdbget" and "jldsdbinsert" scripts

0.4.0: introduced the "CMDNAME" environment variable for logging
       added support for syslog
       added progress report INFO log entries to jlds3upload

0.3.2: added trigger feature to jlds3list

0.3.1: added '-jbn' to jlds3list

0.3.0: introduction of 'jldsqsjob'

0.2.19: added '-em' option to jldrxsqs

0.2.18: added '-se' option to jldtxsqs and jldrxsqs

0.2.17: added '-em' option to jldtxsqs

0.2.16: better formatting of help messages, fixed bug in jldrxsqs

0.2.12 : added '-trm' option to jldrxsqs

0.2.9 : terminate script upon parent termination

0.2.8 :

* added "wait for stdin trigger" to jldrxsqs
* fixed stdout flushing

0.2.7 : forgot to add 'jlds3up' to the scripts...

0.2.6 :

* added 'jlds3download'
* fixed bug in 'jlds3upload'
* new dependency: 'pyfnc'
* added 'jlds3up' : simple file upload

0.2.5 : 

* normalization of options across scripts (see doc page)
* initial release of 'jlds3upload'
* added "check path" functionality - conditional execution of scripts
* initial release of 'jldleader'

0.2.3 : changed command-line format for 'jldrxsqs' and 'jldtxsqs', 'jlds3notify'

0.2.2 :

* addition of "jldrxsqs"
* added parameter '-w' to jldrxsqs and jldtxsqs

0.2.1 :
 
* addition of "json output to stdout" for jlds3notify & jldexec
* addition of "-n" parameter to jldexec : max batch_size per interval
* addition of "jldtxsqs"


0.2.0 : addition of "jlds3notify" script

0.1.1 : initial release with "jldexec" script
