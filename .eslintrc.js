module.exports = {
    "env": {
        "es6": true,
        "node": true,
        "mocha": true
    },
    "extends": "eslint:recommended",
    "parserOptions": {
        "ecmaVersion": 2020,
        "sourceType": "module"
    },
    "rules": {
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
