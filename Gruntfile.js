module.exports = function(grunt) {

	grunt.initConfig({
		pkg: grunt.file.readJSON('package.json'),
		bump: {
			options: {
				pushTo: 'origin',
				files: ['package.json', 'composer.json'],
				commitFiles: ['package.json', 'composer.json']
			}
		}
	});

	grunt.loadNpmTasks('grunt-bump');
};
