// Copyright IBM Corp. 2016,2019. All Rights Reserved.
// Node module: loopback-connector-ibmi
// This file is licensed under the Artistic License 2.0.
// License text available at https://opensource.org/licenses/Artistic-2.0

'use strict';

module.exports = process.env.CI ? describe.skip.bind(describe) : describe;
