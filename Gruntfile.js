/*jslint node: true */
"use strict";

var fsq_dir_index = process.argv.indexOf('--fsq-dir');

module.exports = function (grunt)
{
    grunt.initConfig(
    {
        jslint: {
            all: {
                src: [ 'Gruntfile.js', 'index.js', 'lib/*.js', 'test/**/*.js', 'bench/**/*.js' ],
                directives: {
                    white: true
                }
            }
        },

        cafemocha: {
            default: {
                src: ['test/common.js', 'test/test_spec.js']
            },
            stress: {
                src: ['test/common.js', 'test/multiple_queues_spec.js' ]
            },
            multi: {
                src: ['test/common.js',
                      'test/rabbitmq_bindings.js',
                      'test/rabbitmq_spec.js']
            }
        },

        apidox: {
            input: ['lib/qlobber-fsq.js', 'lib/events_doc.js'],
            output: 'README.md',
            fullSourceDescription: true,
            extraHeadingLevels: 1,
            sections: {
                'QlobberFSQ': '\n## Constructor',
                'QlobberFSQ.prototype.subscribe': '\n## Publish and subscribe',
                'QlobberFSQ.prototype.stop_watching': '\n## Lifecycle',
                'QlobberFSQ.events.start': '\n## Events'
            }
        },

        exec: {
            cover: {
                cmd: './node_modules/.bin/istanbul cover ./node_modules/.bin/grunt -- test ' + (fsq_dir_index < 0 ? /* istanbul ignore next */ '' : process.argv.slice(fsq_dir_index).join(' '))
            },

            check_cover: {
                cmd: './node_modules/.bin/istanbul check-coverage --statement 90 --branch 85 --function 95 --line 95'
            },

            coveralls: {
                cmd: 'cat coverage/lcov.info | coveralls'
            },

            bench: {
                cmd: './node_modules/.bin/bench -c 1 -i "$(echo bench/implementations/*.js | tr " " ,)" --data "' + new Buffer(JSON.stringify(process.argv.slice(3))).toString('hex') + '"'

            },

            diagrams: {
                cmd: 'dot diagrams/how_it_works.dot -Tsvg -odiagrams/how_it_works.svg'
            }
        }
    });
    
    grunt.loadNpmTasks('grunt-jslint');
    grunt.loadNpmTasks('grunt-cafe-mocha');
    grunt.loadNpmTasks('grunt-apidox');
    grunt.loadNpmTasks('grunt-exec');

    grunt.registerTask('lint', 'jslint:all');
    grunt.registerTask('test', 'cafemocha:default');
    grunt.registerTask('test-stress', 'cafemocha:stress');
    grunt.registerTask('test-multi', 'cafemocha:multi');
    grunt.registerTask('docs', ['exec:diagrams', 'apidox']);
    grunt.registerTask('coverage', ['exec:cover', 'exec:check_cover']);
    grunt.registerTask('coveralls', 'exec:coveralls');
    grunt.registerTask('bench', 'exec:bench');
    grunt.registerTask('default', ['jslint', 'cafemocha']);
};
