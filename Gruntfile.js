/*jslint node: true */
"use strict";

const c8 = "npx c8 -x Gruntfile.js -x 'test/**'";

module.exports = function (grunt)
{
    grunt.initConfig(
    {
        eslint: {
            target: [
                'Gruntfile.js',
                'index.js',
                'lib/*.js',
                'test/**/*.js',
                'bench/**/*.js'
            ]
        },

        mochaTest: {
            default: {
                src: [
                    'test/common.js',
                    'test/test_spec.js',
                    'test/lock_spec.js',
                    'test/disruptor_streams_spec.js'
                ]
            },
            stress: {
                src: [
                    'test/common.js',
                    'test/multiple_queues_spec.js'
                ]
            },
            multi: {
                src: [
                    'test/common.js',
                    'test/rabbitmq_bindings.js',
                    'test/rabbitmq_spec.js'
                ]
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

        exec: Object.fromEntries(Object.entries({
            cover: `${c8} grunt test ${process.argv.slice(3).join(' ')}`,
            cover_report: `${c8} report -r lcov`,
            cover_check: `${c8} check-coverage --statements 90 --branches 85 --functions 95 --lines 95`,
            bench: `npx bench -c 1 -i bench/implementations/qlobber-fsq.js --data "${Buffer.from(JSON.stringify(process.argv.slice(3))).toString('hex')}"`,
            diagrams: 'dot diagrams/how_it_works.dot -Tsvg -odiagrams/how_it_works.svg'
        }).map(([k, cmd]) => [k, { cmd, stdio: 'inherit' }]))
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
