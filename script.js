$('.try-me').mouseenter(function(){
  if($(this).find("img:last").is(':hidden')){
    $(this).find("img:last").animate({borderRadius: 30}, 100);
    $(this).find("img:first").animate({borderRadius: 30}, 400);
    $(this).find("img:last").fadeToggle();
  };
});

$('.try-me').mouseleave(function(){
  if($(this).find("img:last").is(':visible')){
    $(this).find("img:last").fadeToggle();
    $(this).find("img:last").animate({borderRadius: 20}, 200);
    $(this).find("img:first").animate({borderRadius: 20}, 200);
  };
});