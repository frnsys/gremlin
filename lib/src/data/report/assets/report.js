document.querySelectorAll(".profiles-tab:first-child, .profiles-profile:first-child")
  .forEach((el) => {
    el.classList.add("focused");
  })
document.querySelectorAll(".profiles")
  .forEach((el) => {
    el.querySelectorAll(".profiles-tab").forEach((el, i) => {
      el.addEventListener("click", () => {
        let top = el.parentElement.parentElement.parentElement;
        top.querySelectorAll(".focused")
          .forEach((ch) => ch.classList.remove("focused"));
        el.classList.add("focused");
        console.log(top);
        console.log(`.profiles-profile:nth-child(${i+1})`);
        top.querySelector(`.profiles-profile:nth-child(${i+1})`).classList.add("focused");
      });
    });
  });

document.querySelectorAll(".step-result").forEach((el) => {
  let inputButton = el.querySelector(".step-toggle-input");
  inputButton.addEventListener("click",  () => {
    el.querySelector(".input-data").style.display = "block";
    el.querySelector(".output-data").style.display = "none";
    el.querySelector(".step-toggle .focused").classList.remove("focused");
    inputButton.classList.add("focused");
  });

  let outputButton = el.querySelector(".step-toggle-output");
  outputButton.addEventListener("click",  () => {
    el.querySelector(".input-data").style.display = "none";
    el.querySelector(".output-data").style.display = "block";
    el.querySelector(".step-toggle .focused").classList.remove("focused");
    outputButton.classList.add("focused");
  });

  let refsButton = el.querySelector(".step-toggle-refs");
  refsButton.addEventListener("click",  () => {
    let isFocused = refsButton.classList.contains("focused");
    if (isFocused) {
      refsButton.classList.remove("focused");
      el.querySelectorAll(".ref-vals").forEach((el) => {
        el.style.display = "none";
      });
    } else {
      refsButton.classList.add("focused");
      el.querySelectorAll(".ref-vals").forEach((el) => {
        el.style.display = "table-row";
      });
    }
  });
});
