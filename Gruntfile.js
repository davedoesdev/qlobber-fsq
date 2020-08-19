/*jslint node: true */
"use strict";

var index = process.argv.indexOf('--napi-modules'),
    args = index < 0 ? '' : process.argv.slice(index).join(' '),
    path = require('path'),
    mod_path = path.join('.', 'node_modules'),
    bin_path = path.join(mod_path, '.bin'),
    nyc_path = path.join(bin_path, 'nyc'),
    grunt_path,
    bench_path;

if (process.platform === 'win32')
{
    grunt_path = path.join(mod_path, 'grunt', 'bin', 'grunt');
    bench_path = path.join(mod_path, 'b', 'bin', 'bench');
}
else
{
    grunt_path = path.join(bin_path, 'grunt');
    bench_path = path.join(bin_path, 'bench');
}

module.exports = function (grunt)
{
    grunt.initConfig(
    {
        eslint: {
            target: [ 'Gruntfile.js', 'index.js', 'lib/*.js', 'test/**/*.js', 'bench/**/*.js' ]
        },

        mochaTest: {
            default: {
                src: ['test/common.js',
                      'test/test_spec.js',
                      'test/lock_spec.js']
            },
            stress: {
                src: ['test/common.js',
                      'test/multiple_queues_spec.js']
            },
            multi: {
                src: ['test/common.js',
                      'test/rabbitmq_bindings.js',
                      'test/rabbitmq_spec.js']
            },
            options: {
                bail: true
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
            },
            doxOptions: { skipSingleStar: true }
        },

        exec: {
            cover: {
                // --napi-modules --harmony-async-iteration should be last
                cmd: nyc_path + " -x Gruntfile.js -x \"" + path.join('test', '**') + "\" node " + args + " " + grunt_path + " test " + process.argv.slice(3).join(' ')
            },

            cover_report: {
                cmd: nyc_path + ' report -r lcov'
            },

            cover_check: {
                cmd: nyc_path + ' check-coverage --statements 90 --branches 85 --functions 95 --lines 95'
            },

            bench: {
                // --napi-modules --harmony-async-iteration should be last
                cmd: 'node ' + args + ' ' + bench_path + ' -c 1 -i bench/implementations/qlobber-fsq.js --data "' + Buffer.from(JSON.stringify(process.argv.slice(3))).toString('hex') + '"'
            },

            diagrams: {
                cmd: 'dot diagrams/how_it_works.dot -Tsvg -odiagrams/how_it_works.svg'
            }
        }
    });
    
    grunt.loadNpmTasks('grunt-eslint');
    grunt.loadNpmTasks('grunt-mocha-test');
    grunt.loadNpmTasks('grunt-apidox');
    grunt.loadNpmTasks('grunt-exec');

    grunt.registerTask('lint', 'eslint');
    grunt.registerTask('test', 'mochaTest:default');
    grunt.registerTask('test-stress', 'mochaTest:stress');
    grunt.registerTask('test-multi', 'mochaTest:multi');
    grunt.registerTask('docs', ['exec:diagrams', 'apidox']);
    grunt.registerTask('coverage', ['exec:cover',
                                    'exec:cover_report',
                                    'exec:cover_check']);
    grunt.registerTask('bench', 'exec:bench');
    grunt.registerTask('default', ['lint', 'mochaTest:default']);
};
