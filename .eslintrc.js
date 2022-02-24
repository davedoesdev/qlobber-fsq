module.exports = {
    "env": {
        "es6": true,
        "node": true,
        "mocha": true
    },
    "extends": "eslint:recommended",
    "parserOptions": {
        "ecmaVersion": 2018,
        "sourceType": "module"
    },
    "rules": {
        "linebreak-style": [
            "error",
            "unix"
        ],
        "semi": [
            "error",
            "always"
        ],
        "brace-style": [
            "error",
            "allman", {
                "allowSingleLine": true
            }
        ],
        "no-unused-vars": [
            "error", {
                "varsIgnorePattern": "^unused_",
                "argsIgnorePattern": "^unused_"
            }
        ]
    }
};
