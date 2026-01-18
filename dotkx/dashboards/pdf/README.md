# PDF server

## Prerequisites

-   Node.js v16.20.0+ (use LTS)

## Installation

-   Run `npm install`
-   It is possible to change the configuration of Puppeteer by modifying `puppeteer.config.js`, see [https://pptr.dev/api/puppeteer.configuration](https://pptr.dev/api/puppeteer.configuration) for more details

## Configuration

-   Settings for pdf generation (timeout, logging) can be changed in `config.js` file

## Troubleshooting

-   During the installation, the correct version of Chrome browser will be downloaded, this requires access to `https://edgedl.me.gvt1.com` domain

-   CentOS dependencies: alsa-lib.x86_64 atk.x86_64 at-spi2-atk cups-libs.x86_64 gtk3.x86_64 ipa-gothic-fonts libXcomposite.x86_64 libXcursor.x86_64 libXdamage.x86_64 libXext.x86_64 libXi.x86_64 libXrandr.x86_64 libXScrnSaver.x86_64 libXtst.x86_64 pango.x86_64 xorg-x11-fonts-100dpi xorg-x11-fonts-75dpi xorg-x11-fonts-cyrillic xorg-x11-fonts-misc xorg-x11-fonts-Type1 xorg-x11-utils

-   [https://pptr.dev/troubleshooting](https://pptr.dev/troubleshooting)






