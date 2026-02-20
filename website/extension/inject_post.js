(function(selectedText){
    const form = document.createElement('form');
    form.method = 'POST';
    form.action = 'http://132.72.112.48:5001/argos';
    form.target = '_self';

    const input = document.createElement('input');
    //input.type = 'hidden';
    input.name = 'text';
    input.value = selectedText;
    form.appendChild(input);

    document.body.appendChild(form);
    form.submit();
})(arguments[0]); // gets selectedText from args