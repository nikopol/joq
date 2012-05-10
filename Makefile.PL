use inc::Module::Install;
name 'joq';
all_from 'lib/joq.pm';

requires 'AnyEvent';
requires 'AnyEvent::Socket';
requires 'EV';
requires 'JSON::XS';
requires 'YAML::XS';
requires 'DateTime';
requires 'Term::ANSIColor','3.0';
requires 'Time::HiRes';
requires 'Getopt::Long';

tests 't/*.t';
#author_tests 'xt';

install_script 'bin/joq';
install_script 'bin/joq-client';

test_requires 'Test::More';
auto_set_repository;
auto_include;
WriteAll;
