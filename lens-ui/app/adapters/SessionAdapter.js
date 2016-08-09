import BaseAdapter from "./BaseAdapter";
import Config from 'config.json';

let baseUrl = Config.baseURL;
let urls = {
    sessionUrl: 'session/sessions'
};
let SessionAdpater = {
    getSessions() {
        let handlesUrl = baseUrl + urls.sessionUrl;
        return BaseAdapter.get(handlesUrl);
    }
};

export default SessionAdpater;