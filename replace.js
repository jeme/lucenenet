const fs = require("fs");
const glob =  require("glob");

const pattern = /Debugging\.Assert\(.*?,\s?\(\)\s?=>.*?\);/g;

glob('**/*.cs', function(err,files){
    if(err){
        console.log(err);
        return;
    }

    files.map(file => {
        const reader = StringReader.open(file);
        const lines = reader.readAllLines();

        let hasReplacements = false;
        const newFile = lines.map(line => {
            return line.line.replace(pattern, str => {
                hasReplacements = true;
                return ReplaceDebuggingCall(str);
            }) + line.break;
        }).join('');
        if(hasReplacements){
            fs.renameSync(file, file + '.orig');
            fs.writeFileSync(file, newFile, 'utf-8');
        }
    });

});

const toStringCall = /^\s*(\w+)\.ToString\(\)\s*$/;
const toStringWithCultureCall = /^\s*(\w+)\.ToString\((CultureInfo\.\w+)\)\s*$/;

/**
 * @return {string}
 */
function ReplaceDebuggingCall(str) {
    const i = str.indexOf(',');
    const pre = str.substring(0, i + 1);
    const post = str.substring(i + 1)
        .replace(/^\s?\(\)\s?=>\s?/, '')
        .replace(/\s?\);\s*$/, '')
        .split(/\s?\+\s?/g);

// Replace:
// >             if (Debugging.AssertsEnabled) Debugging.Assert(((efIndex >= 0) && (efIndex < numEncoded)), () => $"efIndex {efIndex.ToString(CultureInfo.InvariantCulture)}");
// >             if (Debugging.AssertsEnabled) Debugging.Assert(((efIndex >= 0) && (efIndex < numEncoded)),"{0}", $"efIndex {efIndex.ToString(CultureInfo.InvariantCulture)}");


    if(post.length === 1 && post[0].indexOf('ToString') >= 0){
        const ctx = post[0];
        const match = ctx.match(toStringWithCultureCall);
        if(match){
            return pre + ' '+ match[2] + ', ' + match[1] + ');';
        }
        return str;
    } else {
        const inputs = [];
        const format = post.reduce((newline, part) => {
            if(part.startsWith('"')){
                return newline + part.substring(1, part.length-1);
            } else {
                const match = part.match(toStringCall);
                inputs.push(match ? match[1] : part);
                return newline + "{" + (inputs.length-1) + "}";
            }
        }, "");
        inputs.unshift('"' + format + '"');
        return pre +  ' ' + inputs.join(', ') + ');';
    }
}


function StringReader(str){
    const self = this;
    let pos = 0;
    let len = str.length;

    self.reset = function() {
        pos = 0;
    };

    self.readLine = function(){
        if(pos >= len)
            return null;

        let start = pos;
        let end;
        let nr = 0;
        while(true){
            let char = str.charAt(pos);
            end = pos;

            if(char === '\r') {
                pos++;
                if(str.charAt(pos) === '\n') {
                    pos++;
                }
                return { line: str.substring(start, end), break: str.substring(end, pos) };
            }

            if(char === '\n'){
                pos++;
                return { line: str.substring(start, end), break: str.substring(end, pos) };
                // return str.substring(start, end);
            }

            if(pos >= len)
                return { line: str.substring(start, end), break: str.substring(end, pos) };
                // return str.substring(start, end);
            pos++;
        }
    };

    self.readAllLines = function() {
        const lines = [];
        let line;
        while ((line = self.readLine()) !== null) {
            lines.push(line);
        }
        return lines;
    };
}

StringReader.open = function (file, encoding) {
    return new StringReader(fs.readFileSync(file, encoding || 'utf-8'))
};